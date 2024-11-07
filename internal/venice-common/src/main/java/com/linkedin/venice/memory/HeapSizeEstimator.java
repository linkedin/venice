package com.linkedin.venice.memory;

import com.linkedin.venice.common.Measurable;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import com.sun.management.HotSpotDiagnosticMXBean;
import java.lang.management.ManagementFactory;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
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
  private static final Map<Class, Integer> KNOWN_SIZES;
  private static final boolean IS_64_BITS;
  private static final boolean COMPRESSED_OOPS;
  private static final boolean COMPRESSED_CLASS_POINTERS;
  private static final int ALIGNMENT_SIZE;
  private static final int OBJECT_HEADER_SIZE;
  private static final int ARRAY_HEADER_SIZE;
  private static final int POINTER_SIZE;
  private static final int JAVA_MAJOR_VERSION;

  static {
    KNOWN_SIZES = new VeniceConcurrentHashMap<>();

    /** Based on: https://shipilev.net/jvm/objects-inside-out/#_data_types_and_their_representation */
    KNOWN_SIZES.put(boolean.class, 1);
    KNOWN_SIZES.put(byte.class, Byte.BYTES);
    KNOWN_SIZES.put(char.class, Character.BYTES);
    KNOWN_SIZES.put(short.class, Short.BYTES);
    KNOWN_SIZES.put(int.class, Integer.BYTES);
    KNOWN_SIZES.put(float.class, Float.BYTES);
    KNOWN_SIZES.put(long.class, Long.BYTES);
    KNOWN_SIZES.put(double.class, Double.BYTES);

    /**
     * This prop name looks sketchy, but I've seen it mentioned in a couple of posts online, so I'm trusting it for
     * now... In any case, if that property is not found, then we'll default to 64 bits, which is a conservative
     * assumption (i.e., it would cause us to over-estimate the size on heap)...
     */
    String propertyName = "sun.arch.data.model";
    String arch = System.getProperty(propertyName);
    LOGGER.info("System property {} has value: {}", propertyName, arch);
    IS_64_BITS = (arch == null) || !arch.contains("32");
    COMPRESSED_OOPS = getBooleanVmOption("UseCompressedOops");
    COMPRESSED_CLASS_POINTERS = getBooleanVmOption("UseCompressedClassPointers");
    ALIGNMENT_SIZE = IS_64_BITS ? 8 : 4; // Also serves as the object header's "mark word" size
    OBJECT_HEADER_SIZE = ALIGNMENT_SIZE + (COMPRESSED_CLASS_POINTERS ? 4 : 8);
    /**
     * The "array base" is always word-aligned, which under some circumstances (in 32 bits JVMs, or in 64 bits JVMs
     * where {@link COMPRESSED_OOPS} is false, either due to a large heap or to explicit disabling) causes "internal
     * space loss".
     *
     * See: https://shipilev.net/jvm/objects-inside-out/#_observation_array_base_is_aligned
     */
    ARRAY_HEADER_SIZE = roundUpToNearest(OBJECT_HEADER_SIZE + Integer.BYTES, ALIGNMENT_SIZE);
    POINTER_SIZE = COMPRESSED_OOPS ? 4 : 8;
    JAVA_MAJOR_VERSION = Utils.getJavaMajorVersion();
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
   * @param c The {@link Class} for which to predict the "base overhead", as defined above.
   * @return The base overhead of the class, which can be any positive number (including zero).
   *
   * @throws {@link StackOverflowError} It should be noted that this function is recursive in nature and so any class
   *         which contains a loop in its class graph will cause a stack overflow.
   */
  public static int getClassOverhead(@Nonnull final Class<?> c) {
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

    Integer knownSize = KNOWN_SIZES.get(c);

    if (knownSize != null) {
      return knownSize;
    }

    int size = c.isArray() ? ARRAY_HEADER_SIZE : OBJECT_HEADER_SIZE;

    /**
     * We need to measure the overhead of fields for the passed in class as well as all parents. The order in which we
     * traverse these classes matters in older Java versions (prior to 15) and not in newer ones, but for the sake of
     * minimizing code size, we will always iterate in the order relevant to older Java versions.
     */
    List<Class> classHierarchyFromSubclassToParent = new ArrayList<>();
    classHierarchyFromSubclassToParent.add(c);
    Class parentClass = c.getSuperclass();
    while (parentClass != null) {
      classHierarchyFromSubclassToParent.add(parentClass);
      parentClass = parentClass.getSuperclass();
    }

    // Iterate from the end to the beginning, so we go from parent to sub
    for (int i = classHierarchyFromSubclassToParent.size() - 1; i >= 0; i--) {
      int classFieldsOverhead = overheadOfFields(classHierarchyFromSubclassToParent.get(i));
      if (classFieldsOverhead == 0) {
        continue;
      }
      size += classFieldsOverhead;

      if (JAVA_MAJOR_VERSION < 15) {
        /**
         * In older Java versions, the field layout would always order the fields from the parent class to subclass.
         * Within a class, the fields could be re-ordered to optimize packing the fields within the "alignment shadow",
         * but not across classes of the hierarchy.
         *
         * See: https://shipilev.net/jvm/objects-inside-out/#_superclass_gaps
         */
        if (i > 0) {
          /**
           * We align for each class, except the last one, since we'll take care of it below. BUT, importantly, at this
           * stage we align by pointer size, NOT by alignment size.
           */
          size = roundUpToNearest(size, POINTER_SIZE);
        }
      }
    }

    // We align once at the end no matter the Java version
    size = roundUpToNearest(size, ALIGNMENT_SIZE);

    KNOWN_SIZES.putIfAbsent(c, size);

    return size;
  }

  /** Deal with alignment by rounding up to the nearest boundary. */
  private static int roundUpToNearest(int size, int intervalSize) {
    int partialAlignmentWindowUsage = size % intervalSize;
    int waste = partialAlignmentWindowUsage == 0 ? 0 : intervalSize - partialAlignmentWindowUsage;
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
    return IS_64_BITS;
  }

  /** Package-private on purpose, for tests... */
  static boolean isUseCompressedOopsEnabled() {
    return COMPRESSED_OOPS;
  }

  /** Package-private on purpose, for tests... */
  static boolean isCompressedKlassPointersEnabled() {
    return COMPRESSED_CLASS_POINTERS;
  }

  private static boolean getBooleanVmOption(String optionName) {
    String optionValue =
        ManagementFactory.getPlatformMXBean(HotSpotDiagnosticMXBean.class).getVMOption(optionName).getValue();
    LOGGER.info("VM option {} has value: {}", optionName, optionValue);
    return Boolean.parseBoolean(optionValue);
  }
}
