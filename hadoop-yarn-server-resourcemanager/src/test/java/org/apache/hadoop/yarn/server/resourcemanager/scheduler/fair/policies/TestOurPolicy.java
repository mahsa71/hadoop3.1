package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.policies;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Comparator;
import java.util.Map;

import org.apache.curator.shaded.com.google.common.base.Joiner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FSContext;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FakeSchedulable;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.Schedulable;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.policies.Policy1.TestComparator;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.policies.Policy1.TestComparator2;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


public class TestOurPolicy {
	 @Before
	 public void setup() {
		   		  }
	 private Comparator<Schedulable> createComparator(int clusterMem,
		      int clusterCpu) {
		    Policy1 policy =
		        new Policy1();
		    FSContext fsContext = mock(FSContext.class);
		    when(fsContext.getClusterResource()).
		        thenReturn(Resources.createResource(clusterMem, clusterCpu));
		    policy.initialize(fsContext);
		    return policy.getComparator();
		  }
	 private Schedulable createSchedulable(int memUsage, int cpuUsage) {
		    return createSchedulable(memUsage, cpuUsage, 1.0f, 0, 0);
		  }
		  
		  private Schedulable createSchedulable(int memUsage, int cpuUsage,
		      int minMemShare, int minCpuShare) {
		    return createSchedulable(memUsage, cpuUsage, 1.0f,
		        minMemShare, minCpuShare);
		  }
		  
		  private Schedulable createSchedulable(int memUsage, int cpuUsage,
		      float weights) {
		    return createSchedulable(memUsage, cpuUsage, weights, 0, 0);
		  }

		  private Schedulable createSchedulable(int memUsage, int cpuUsage,
		      float weights, int minMemShare, int minCpuShare) {
		    Resource usage = BuilderUtils.newResource(memUsage, cpuUsage);
		    Resource minShare = BuilderUtils.newResource(minMemShare, minCpuShare);
		    return new FakeSchedulable(minShare,
		        Resources.createResource(Integer.MAX_VALUE, Integer.MAX_VALUE),
		        weights, Resources.none(), usage, 0l);
		  }
		  
	@Test
	 public void testNoneNeedy() {
	    Comparator<Schedulable> c = createComparator(8000, 8);
	    Schedulable s1 = createSchedulable(4000, 3);
	    Schedulable s2 = createSchedulable(2000, 5);

	    assertTrue("Comparison didn't return a value less than 0",
	        c.compare(s1, s2)< 0);
	  }
	@Test
	public void testNoneNeedy2() {
	    ResourceUtils.resetResourceTypes(new Configuration());
	    testNoneNeedy();
	  }
	

}
