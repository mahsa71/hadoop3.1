package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.policies;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Comparator;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FSContext;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler;
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
public class Test1PolicyTest {
	
	private static int totalUsedMemory = 0 ;
	private static int totalUsedVcore = 0 ;
	private static int totalMemory = 8000 ;
	private static int totalVcore = 100 ;
	
	@Before
	public void setUp() throws Exception {
	}

	private Comparator<Schedulable> createComparator(int clusterMem,
		      int clusterCpu) {
			 
			Policy1 policy = new Policy1() ;

		    FSContext fsContext = mock(FSContext.class);
		    when(fsContext.getClusterUsage()).
	        thenReturn(Resources.createResource(totalUsedMemory, totalUsedVcore));
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
			 public void test0neNeedy() {
					Comparator<Schedulable>  c1 = createComparator(totalMemory, totalVcore);
				    Schedulable s1 = createSchedulable(200, 1,300, 2);	    
				    Schedulable s2 = createSchedulable(100, 2);	   
				    int res = c1.compare(s1, s2);
				    if (res < 0){
				    	updateClustrUsage(200, 1);
				    }else{
				    	updateClustrUsage(100, 2);
				    }
				  /*  
				    Comparator<Schedulable>  c2 = createComparator(totalMemory, totalVcore);
				    Schedulable s3 = createSchedulable(200, 1,300, 2);	    
				    Schedulable s4 = createSchedulable(100, 2);	   
				     res = c2.compare(s3, s4);
				    if (res < 0){
				    	updateClustrUsage(200, 1);
				    }else{
				    	updateClustrUsage(100, 2);
				    }*/
				System.out.println("************** finished S1 is needy *******************");
				
				
			}  
	@Test
	 public void testNoneNeedy() {
	//	test0neNeedy();
		Comparator<Schedulable> c1;
		while((totalUsedMemory < totalMemory)&&
				(totalUsedVcore) < totalVcore){
			 c1 = createComparator(totalMemory, totalVcore);
		    Schedulable s1 = createSchedulable(200, 1);	    
		    Schedulable s2 = createSchedulable(100, 2);	   
		    int res = c1.compare(s1, s2);
		    if (res < 0){
		    	updateClustrUsage(200, 1);
		    }else{
		    	updateClustrUsage(100, 2);
		    }
		    
		}
		System.out.println("************** cluster is full *******************");
		
		
	}
	@Test
	public void testNoneNeedy2() {
	    ResourceUtils.resetResourceTypes(new Configuration());
	    testNoneNeedy();
	  }
	public void updateClustrUsage(int mem , int cpu){
		totalUsedMemory+= mem ;
		totalUsedVcore+= cpu;
	}

}
