package com.linkedin.venice.authorization;

import com.linkedin.venice.common.VeniceSystemStoreType;
import java.util.Arrays;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Test class for authorization related classes, AceEntry, AclBinding, etc.
 */
public class AuthorizationTest {
  @Test
  public void testAceEntry() {

    Principal p1 = new Principal("user:user1");
    Principal p2 = new Principal("user:user2");
    Principal p3 = new Principal("user:user1");
    Assert.assertEquals(p1, p3);
    Assert.assertNotEquals(p1, p2);

    AceEntry ace1 = new AceEntry(p1, Method.Read, Permission.ALLOW);
    AceEntry ace2 = new AceEntry(p1, Method.Write, Permission.ALLOW);
    AceEntry ace3 = new AceEntry(p1, Method.Read, Permission.DENY);
    AceEntry ace4 = new AceEntry(p2, Method.Read, Permission.ALLOW);
    AceEntry ace5 = new AceEntry(new Principal("user:user1"), Method.Read, Permission.ALLOW);
    Assert.assertEquals(ace1.getPrincipal(), p1);
    Assert.assertEquals(ace1.getMethod(), Method.Read);
    Assert.assertEquals(ace1.getPermission(), Permission.ALLOW);

    Assert.assertEquals(ace1, ace1);
    Assert.assertEquals(ace1, ace5);

    Assert.assertNotEquals(ace1, ace2);
    Assert.assertNotEquals(ace1, ace3);
    Assert.assertNotEquals(ace1, ace4);

    Resource r1 = new Resource("store1");
    Resource r2 = new Resource("store2");
    Assert.assertEquals(r1, new Resource("store1"));
    Assert.assertNotEquals(r1, r2);

    AclBinding ab1 = new AclBinding(r1);
    Assert.assertEquals(ab1.countAceEntries(), 0);
    ab1.addAceEntry(ace1);
    Assert.assertEquals(ab1.getAceEntries().iterator().next(), ace1);

    ab1.addAceEntries(Arrays.asList(ace2, ace3));
    Assert.assertEquals(ab1.countAceEntries(), 3);
  }

  @Test
  public void testAclBinding() {
    Resource r1 = new Resource("store1");
    Resource r2 = new Resource("store2");

    Assert.assertEquals(r1, new Resource("store1"));
    Assert.assertNotEquals(r1, r2);

    AclBinding ab1 = new AclBinding(r1);
    Assert.assertEquals(ab1.countAceEntries(), 0);

    AceEntry ace1 = new AceEntry(new Principal("user:user1"), Method.Read, Permission.ALLOW);
    AceEntry ace2 = new AceEntry(new Principal("user:user2"), Method.Read, Permission.ALLOW);
    ab1.addAceEntry(ace1);
    ab1.addAceEntry(ace2);
    Assert.assertEquals(ab1.countAceEntries(), 2);

    ab1.addAceEntry(ace2);
    Assert.assertEquals(ab1.countAceEntries(), 3);

    ab1.removeAceEntry(ace2);
    Assert.assertEquals(ab1.countAceEntries(), 2);
  }

  @Test
  public void testSystemStoreAclBinding() {
    String storeName = "store1";
    Resource r1 = new Resource(storeName);
    Principal p1 = new Principal("user:user1");
    Principal p2 = new Principal("user:user2");
    Principal p3 = new Principal("user:user3");

    AclBinding ab = new AclBinding(r1);
    ab.addAceEntry(new AceEntry(p1, Method.Read, Permission.ALLOW));
    ab.addAceEntry(new AceEntry(p2, Method.Write, Permission.ALLOW));
    ab.addAceEntry(new AceEntry(p3, Method.Read, Permission.ALLOW));

    for (VeniceSystemStoreType veniceSystemStoreType: VeniceSystemStoreType.values()) {
      AclBinding sysAb = veniceSystemStoreType.generateSystemStoreAclBinding(ab);
      AclBinding sysKafkaTopicAb = new AclBinding(new Resource(veniceSystemStoreType.getSystemStoreName(storeName)));
      sysKafkaTopicAb.addAceEntry(
          new AceEntry(
              p1,
              veniceSystemStoreType.getClientAccessMethod() == Method.READ_SYSTEM_STORE ? Method.Read : Method.Write,
              Permission.ALLOW));
      sysKafkaTopicAb.addAceEntry(
          new AceEntry(
              p3,
              veniceSystemStoreType.getClientAccessMethod() == Method.READ_SYSTEM_STORE ? Method.Read : Method.Write,
              Permission.ALLOW));
      Assert.assertEquals(sysAb.getResource().getName(), veniceSystemStoreType.getSystemStoreName(storeName));
      Assert.assertEquals(sysAb.getAceEntries().size(), 2);
      Assert.assertTrue(
          sysAb.getAceEntries()
              .contains(new AceEntry(p1, veniceSystemStoreType.getClientAccessMethod(), Permission.ALLOW)));
      Assert.assertFalse(
          sysAb.getAceEntries()
              .contains(new AceEntry(p2, veniceSystemStoreType.getClientAccessMethod(), Permission.ALLOW)));
      Assert.assertTrue(
          sysAb.getAceEntries()
              .contains(new AceEntry(p3, veniceSystemStoreType.getClientAccessMethod(), Permission.ALLOW)));
      Assert.assertEquals(sysAb, VeniceSystemStoreType.getSystemStoreAclFromTopicAcl(sysKafkaTopicAb));
    }
  }
}
