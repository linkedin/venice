package com.linkedin.venice.authorization;

import java.util.Arrays;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


/**
 * Test class for authorization related classes, AceEntry, AclBinding, etc.
 */
public class AuthorizationTest {

  @Test
  public void testAceEntry() {

    Principal p1 = new Principal("urn:li:corpuser:user1");
    Principal p2 = new Principal("urn:li:corpuser:user2");
    Principal p3 = new Principal("urn:li:corpuser:user1");
    Assert.assertEquals(p1, p3);
    Assert.assertNotEquals(p1, p2);

    AceEntry ace1 = new AceEntry(p1, Method.Read, Permission.ALLOW);
    AceEntry ace2 = new AceEntry(p1, Method.Write, Permission.ALLOW);
    AceEntry ace3 = new AceEntry(p1, Method.Read, Permission.DENY);
    AceEntry ace4 = new AceEntry(p2, Method.Read, Permission.ALLOW);
    AceEntry ace5 = new AceEntry(new Principal("urn:li:corpuser:user1"), Method.Read, Permission.ALLOW);
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

    AceEntry ace1 = new AceEntry(new Principal("urn:li:corpuser:user1"), Method.Read, Permission.ALLOW);
    AceEntry ace2 = new AceEntry(new Principal("urn:li:corpuser:user2"), Method.Read, Permission.ALLOW);
    ab1.addAceEntry(ace1);
    ab1.addAceEntry(ace2);
    Assert.assertEquals(ab1.countAceEntries(), 2);

    ab1.addAceEntry(ace2);
    Assert.assertEquals(ab1.countAceEntries(), 3);

    ab1.removeAceEntry(ace2);
    Assert.assertEquals(ab1.countAceEntries(), 2);
  }
}
