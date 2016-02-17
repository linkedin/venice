package com.linkedin.venice.meta;

import java.util.List;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Test cases for Venice Store.
 */
public class TestStore {
    @Test
    public void testVersionsAreAddedInOrdered(){
        Store s = new Store("s1","owner",System.currentTimeMillis());
        s.addVersion(new Version(s.getName(),4,System.currentTimeMillis()));
        s.addVersion(new Version(s.getName(),2,System.currentTimeMillis()));
        s.addVersion(new Version(s.getName(),3,System.currentTimeMillis()));
        s.addVersion(new Version(s.getName(),1,System.currentTimeMillis()));

        List<Version> versions = s.getVersions();
        Assert.assertEquals(4,versions.size());
        for(int i=0;i<versions.size();i++){
           Assert.assertEquals(i+1,versions.get(i).getNumber());
        }
    }

    @Test
    public void testDeleteVersion(){
        Store s = new Store("s1","owner",System.currentTimeMillis());
        s.addVersion(new Version(s.getName(),4,System.currentTimeMillis()));
        s.addVersion(new Version(s.getName(),2,System.currentTimeMillis()));
        s.addVersion(new Version(s.getName(),3,System.currentTimeMillis()));
        s.addVersion(new Version(s.getName(), 1, System.currentTimeMillis()));

        s.deleteVersion(3);
        List<Version> versions = s.getVersions();
        Assert.assertEquals(3, versions.size());
        for(int i: new int[] {2,3,4}){
            Assert.assertEquals(i,versions.get(i).getNumber());
        }
    }

    @Test
    public void testCloneStore(){
        Store s = new Store("s1","owner",System.currentTimeMillis());
        Store clonedStore = s.cloneStore();
        Assert.assertTrue(s.equals(clonedStore));
        clonedStore.setCurrentVersion(100);
        Assert.assertEquals(0,s.getCurrentVersion());
    }
}
