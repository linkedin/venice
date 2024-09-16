package com.linkedin.venice.utils;

/**
 * N.B.: Although there is no way to force this via Java interfaces, the convention is that all enums implementing this
 * interface should have static "valueOf" function to return the correct enum value from a given numeric value, i.e.:
 *
 * {@snippet id='valueOf':
 * public static MyEnumType valueOf(int value) {
 *   return EnumUtils.valueOf(TYPES_ARRAY, value, MyEnumTpe.class);
 * }
 * }
 *
 * Note that VeniceEnumValueTest makes it easy to test the above, and we should have a subclass of that test for all
 * implementations of this interface.
 */
public interface VeniceEnumValue {
  int getValue();
}
