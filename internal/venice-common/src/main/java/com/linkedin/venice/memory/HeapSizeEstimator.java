package com.linkedin.venice.memory;

import com.linkedin.venice.common.Measurable;
import com.sun.management.HotSpotDiagnosticMXBean;
import java.lang.management.ManagementFactory;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Utility class to help in implementing {@link Measurable#getSize()}. A couple of important points:
 *
 * 1. This utility class does not "measure" the heap size, but rather attempts to "predict" it, based on knowledge of
 *    the internals of the JVM. If any of the assumptions are wrong, then of course the results will be inaccurate.
 * 2. This utility class assumes we are using the HotSpot JVM.
 */
public class HeapSizeEstimator {
  private static final Logger LOGGER = LogManager.getLogger(HeapSizeEstimator.class);
  private static final Map<Class, Integer> PRIMITIVE_SIZES;
  private static final int ALIGNMENT_SIZE;
  private static final int OBJECT_HEADER_SIZE;
  private static final int ARRAY_HEADER_SIZE;
  private static final int POINTER_SIZE;

  static {
    Map<Class, Integer> modifiablePrimitiveSizesMap = new HashMap<>();

    /** Based on: https://shipilev.net/jvm/objects-inside-out/#_data_types_and_their_representation */
    modifiablePrimitiveSizesMap.put(boolean.class, 1);
    modifiablePrimitiveSizesMap.put(byte.class, Byte.BYTES);
    modifiablePrimitiveSizesMap.put(char.class, Character.BYTES);
    modifiablePrimitiveSizesMap.put(short.class, Short.BYTES);
    modifiablePrimitiveSizesMap.put(int.class, Integer.BYTES);
    modifiablePrimitiveSizesMap.put(float.class, Float.BYTES);
    modifiablePrimitiveSizesMap.put(long.class, Long.BYTES);
    modifiablePrimitiveSizesMap.put(double.class, Double.BYTES);

    boolean is64bitsJVM = is64bitsJVM();
    int markWordSize = is64bitsJVM ? 8 : 4;
    boolean isCompressedOopsEnabled = isUseCompressedOopsEnabled();
    boolean isCompressedKlassPointersEnabled = isCompressedKlassPointersEnabled();
    int classPointerSize = isCompressedKlassPointersEnabled ? 4 : 8;

    PRIMITIVE_SIZES = Collections.unmodifiableMap(modifiablePrimitiveSizesMap);
    ALIGNMENT_SIZE = is64bitsJVM ? 8 : 4;
    OBJECT_HEADER_SIZE = markWordSize + classPointerSize;
    /**
     * The "array base" is always word-aligned, which under some circumstances (in 32 bits JVMs, or in 64 bits JVMs
     * where {@link isCompressedOopsEnabled} is false, either due to large heap or to explicit disabling) causes
     * "internal space loss".
     *
     * See: https://shipilev.net/jvm/objects-inside-out/#_observation_array_base_is_aligned
     */
    ARRAY_HEADER_SIZE = roundUpToNearestAlignment(OBJECT_HEADER_SIZE + Integer.BYTES);
    POINTER_SIZE = isCompressedOopsEnabled ? 4 : 8;
  }

  /**
   * This function is a helper to make it easier to implement {@link Measurable#getSize()}. It provides the "overhead"
   * of each instance of the given class, while making the following assumptions:
   *
   * 1. No pointer is null
   * 2. Any variable sized objects (e.g., arrays and other collections) are empty
   * 3. Polymorphism is ignored (i.e., we do not guess the size of any potential implementation of an abstraction)
   *
   * The intended usage of this function is to call it once per runtime per class of interest and to store the result in
   * a static field. If the instances of this class (and any other class contained within it) contain no fields which
   * are variable-sized object, no nullable objects, and no polymorphism, then the result of this function is
   * effectively the heap size of each instance. If any of these conditions are not true, then a function should be
   * implemented which uses this class overhead as a base and then makes adjustments, e.g.:
   *
   * 1. For any null pointer, we can subtract from the base a value equal to the output of this function for the type of
   *    the field which is null.
   * 2. For any variable-sized object, then the actual size has to be taken into account.
   * 3. If polymorphism comes into play, then the specific sizes of actual instances need to take into account the
   *    concrete classes they are made of.
   *
   * @throws {@link StackOverflowError} It should be noted that this function is recursive in nature and so any class
   *         which contains a loop in its class graph will cause a stack overflow.
   */
  public static <T> int getClassOverhead(Class<T> c) {
    if (c == null) {
      throw new NullPointerException("The class param must not be null.");
    }

    if (c.isEnum()) {
      /**
       * Each enum value is stored statically, and so the per-instance overhead of enum fields is only the pointer size,
       * already taken into account in the field loop below.
       */
      return 0;
    }

    if (c.isPrimitive()) {
      return PRIMITIVE_SIZES.get(c);
    }

    int size = c.isArray() ? ARRAY_HEADER_SIZE : OBJECT_HEADER_SIZE;

    size += overheadOfFields(c);

    // TODO: Fix imprecision due to internal loss in parent class layouts
    Class parentClass = c.getSuperclass();
    while (parentClass != null) {
      size += overheadOfFields(parentClass);
      parentClass = parentClass.getSuperclass();
    }

    int finalSize = roundUpToNearestAlignment(size);

    return finalSize;
  }

  /** Deal with alignment by rounding up to the nearest alignment boundary. */
  private static int roundUpToNearestAlignment(int size) {
    int partialAlignmentWindowUsage = size % ALIGNMENT_SIZE;
    int waste = partialAlignmentWindowUsage == 0 ? 0 : ALIGNMENT_SIZE - partialAlignmentWindowUsage;
    int finalSize = size + waste;
    return finalSize;
  }

  private static int overheadOfFields(Class c) {
    int size = 0;

    /**
     * {@link Class#getDeclaredFields()} returns the fields (of all visibility, from private to public and everything
     * in between) of the class, but not of its parent class.
     */
    for (Field f: c.getDeclaredFields()) {
      if (Modifier.isStatic(f.getModifiers())) {
        continue;
      }
      Class fieldClass = f.getType();
      if (!fieldClass.isPrimitive()) {
        /**
         * Only primitives are stored in-line within the object, while all non-primitives are stored elsewhere on the
         * heap, with a pointer within the object to reference them.
         */
        size += POINTER_SIZE;
      }
      size += getClassOverhead(fieldClass);
    }

    return size;
  }

  /** Package-private on purpose, for tests... */
  static boolean is64bitsJVM() {
    /**
     * This prop name looks sketchy, but I've seen it mentioned in a couple of posts online, so I'm trusting it for
     * now... In any case, if that property is not found, then we'll default to 64 bits, which is a conservative
     * assumption (i.e., it would cause us to over-estimate the size on heap)...
     */
    String propertyName = "sun.arch.data.model";
    String arch = System.getProperty(propertyName);
    LOGGER.info("System property {} has value: {}", propertyName, arch);

    return (arch == null) || !arch.contains("32");
  }

  /** Package-private on purpose, for tests... */
  static boolean isUseCompressedOopsEnabled() {
    // Simpler method, but doesn't compile, for some reason...
    // return sun.jvm.hotspot.runtime.VM.getVM().isCompressedOopsEnabled();

    return getBooleanVmOption("UseCompressedOops");
  }

  /** Package-private on purpose, for tests... */
  static boolean isCompressedKlassPointersEnabled() {
    // Simpler method, but doesn't compile, for some reason...
    // return sun.jvm.hotspot.runtime.VM.getVM().isCompressedKlassPointersEnabled();

    return getBooleanVmOption("UseCompressedClassPointers");
  }

  private static boolean getBooleanVmOption(String optionName) {
    String optionValue =
        ManagementFactory.getPlatformMXBean(HotSpotDiagnosticMXBean.class).getVMOption(optionName).getValue();
    LOGGER.info("VM option {} has value: {}", optionName, optionValue);
    return Boolean.parseBoolean(optionValue);
  }
}
