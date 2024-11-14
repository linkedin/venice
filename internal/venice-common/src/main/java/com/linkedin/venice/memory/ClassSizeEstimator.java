package com.linkedin.venice.memory;

import com.linkedin.venice.utils.Utils;
import com.sun.management.HotSpotDiagnosticMXBean;
import java.lang.management.ManagementFactory;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nonnull;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Utility class to help in implementing {@link Measurable#getHeapSize()}. A couple of important points:
 *
 * 1. This utility class does not "measure" the heap size, but rather attempts to "predict" it, based on knowledge of
 *    the internals of the JVM. If any of the assumptions are wrong, then of course the results will be inaccurate.
 * 2. This utility class assumes we are using the HotSpot JVM.
 */
public class ClassSizeEstimator {
  private static final Logger LOGGER = LogManager.getLogger(ClassSizeEstimator.class);
  private static final ClassValue<Integer> KNOWN_SHALLOW_SIZES = new ClassValue<Integer>() {
    @Override
    protected Integer computeValue(Class<?> type) {
      return computeClassOverhead(type);
    }
  };
  private static final boolean IS_64_BITS;
  private static final boolean COMPRESSED_OOPS;
  private static final boolean COMPRESSED_CLASS_POINTERS;
  private static final int ALIGNMENT_SIZE;
  private static final int OBJECT_HEADER_SIZE;
  static final int ARRAY_HEADER_SIZE;
  private static final int POINTER_SIZE;
  private static final int JAVA_MAJOR_VERSION;

  static {
    /**
     * This prop name looks sketchy, but I've seen it mentioned in a couple of posts online, so I'm trusting it for
     * now... In any case, if that property is not found, then we'll default to 64 bits, which is a conservative
     * assumption (i.e., it would cause us to over-estimate the size on heap)...
     */
    String propertyName = "sun.arch.data.model";
    String arch = System.getProperty(propertyName);
    LOGGER.debug("System property {} has value: {}", propertyName, arch);
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

  private ClassSizeEstimator() {
    // Static utility
  }

  /**
   * This function provides the "shallow size" of each instance of the given class, meaning that all pointers are
   * considered to be null.
   *
   * Intended usage: This function is a helper to make it easier to implement {@link Measurable#getHeapSize()}. It
   * should not be called on the hot path. Rather, it should be called at most once per JVM runtime per class of
   * interest and the result should be stored in a static field. If an instance of a measured class contains only
   * primitive fields or all its non-primitive fields are null, then the result of this function is effectively the heap
   * size of that instance. If these conditions are not true, then a function should be implemented which uses this
   * class overhead as a base and then adds the size of any referenced Objects. For example, see
   * {@link InstanceSizeEstimator#getObjectSize(Object)}.
   *
   * @param c The {@link Class} for which to predict the "shallow overhead", as defined above.
   * @return The base overhead of the class, which can be any positive number (including zero).
   */
  public static int getClassOverhead(@Nonnull final Class<?> c) {
    Integer knownSize = KNOWN_SHALLOW_SIZES.get(c);
    if (knownSize != null) {
      return knownSize;
    }
    return computeClassOverhead(c);
  }

  private static int computeClassOverhead(@Nonnull final Class<?> c) {
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
      return getPrimitiveSize(c);
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
    size = roundUpToNearestAlignment(size);

    return size;
  }

  /** Based on: https://shipilev.net/jvm/objects-inside-out/#_data_types_and_their_representation */
  private static int getPrimitiveSize(Class<?> c) {
    if (c.isPrimitive()) {
      if (c.equals(boolean.class)) {
        return 1;
      } else if (c.equals(byte.class)) {
        return Byte.BYTES;
      } else if (c.equals(char.class)) {
        return Character.BYTES;
      } else if (c.equals(short.class)) {
        return Short.BYTES;
      } else if (c.equals(int.class)) {
        return Integer.BYTES;
      } else if (c.equals(float.class)) {
        return Float.BYTES;
      } else if (c.equals(long.class)) {
        return Long.BYTES;
      } else if (c.equals(double.class)) {
        return Double.BYTES;
      }

      // Defensive code
      throw new IllegalStateException(
          "Class " + c.getSimpleName()
              + " is said to be a primitive but does not conform to any known primitive type!");
    } else {
      throw new IllegalArgumentException("Class " + c.getSimpleName() + " is not a primitive!");
    }
  }

  static int roundUpToNearestAlignment(int size) {
    return roundUpToNearest(size, ALIGNMENT_SIZE);
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
      if (fieldClass.isPrimitive()) {
        size += KNOWN_SHALLOW_SIZES.get(fieldClass);
      } else {
        /**
         * Only primitives are stored in-line within the object, while all non-primitives are stored elsewhere on the
         * heap, with a pointer within the object to reference them.
         */
        size += POINTER_SIZE;
      }
    }

    /**
     * N.B.: The output of this function could be cached, though we're currently not doing it since the intent is not
     * to call this on the hat path anyway.
     */

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
    LOGGER.debug("VM option {} has value: {}", optionName, optionValue);
    return Boolean.parseBoolean(optionValue);
  }
}
