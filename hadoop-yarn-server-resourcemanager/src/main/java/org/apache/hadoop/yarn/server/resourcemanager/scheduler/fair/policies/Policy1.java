package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.policies;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;

import com.google.common.annotations.VisibleForTesting;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FSContext;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.Schedulable;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.SchedulingPolicy;
import org.apache.hadoop.yarn.util.resource.DominantResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Makes scheduling decisions by trying to equalize dominant resource usage.
 * A schedulable's dominant resource usage is the largest ratio of resource
 * usage to capacity among the resource types it is using.
 */
@Private
@Unstable
public class Policy1 extends SchedulingPolicy {
	
	 public static final String NAME = "Test1";

	    private static final Log LOG = LogFactory.getLog(Policy1.class);
		private static final int NUM_RESOURCES =
			      ResourceUtils.getNumberOfKnownResourceTypes();
		private static final TestComparator2 COMPARATOR =
			      new TestComparator2();
		private static final DominantResourceCalculator CALCULATOR =
			      new DominantResourceCalculator();
		private static ArrayList<Schedulable>  schedulables = new ArrayList<Schedulable>() ;
		private static ArrayList<Integer>  fitnessOfAll = new ArrayList<Integer>() ;
		 private static double temperature = 0.001;


		

	  @Override
	  public String getName() {
	    return NAME;
	  }

	  @Override
	  public Comparator<Schedulable> getComparator() {
	    
	      return COMPARATOR;
	    

	  }

	  @Override
	  public ResourceCalculator getResourceCalculator() {
	    return CALCULATOR;
	  }

	  @Override
	  public void computeShares(Collection<? extends Schedulable> schedulables,
	      Resource totalResources) {
	    for (ResourceInformation info: ResourceUtils.getResourceTypesArray()) {
	      ComputeFairShares.computeShares(schedulables, totalResources,
	          info.getName());
	    }
	  }

	  @Override
	  public void computeSteadyShares(Collection<? extends FSQueue> queues,
	      Resource totalResources) {
	    for (ResourceInformation info: ResourceUtils.getResourceTypesArray()) {
	      ComputeFairShares.computeSteadyShares(queues, totalResources,
	          info.getName());
	    }
	  }

	  @Override
	  public boolean checkIfUsageOverFairShare(Resource usage, Resource fairShare) {
	    return !Resources.fitsIn(usage, fairShare);
	  }

	  @Override
	  public Resource getHeadroom(Resource queueFairShare, Resource queueUsage,
	                              Resource maxAvailable) {
	    long queueAvailableMemory =
	        Math.max(queueFairShare.getMemorySize() - queueUsage.getMemorySize(), 0);
	    int queueAvailableCPU =
	        Math.max(queueFairShare.getVirtualCores() - queueUsage
	            .getVirtualCores(), 0);
	    Resource headroom = Resources.createResource(
	        Math.min(maxAvailable.getMemorySize(), queueAvailableMemory),
	        Math.min(maxAvailable.getVirtualCores(),
	            queueAvailableCPU));
	    return headroom;
	  }

	  @Override
	  public void initialize(FSContext fsContext) {
		    COMPARATOR.setFSContext(fsContext);
		  }
		
	  static double calculateTemperature() {
	 	     int max = Collections.max(fitnessOfAll);
	 	     int min = Collections.min(fitnessOfAll);
	 	     int entropy = max - min ;
	 	     double temperature = entropy * 1000 ;
	 	   return temperature;
	    }


	  /**
	   * This class compares two {@link Schedulable} instances according to the
	   * DRF policy. If neither instance is below min share, approximate fair share
	   * ratios are compared. Subclasses of this class will do the actual work of
	   * the comparison, specialized for the number of configured resource types.
	   */
	  public abstract static class TestComparator
	      implements Comparator<Schedulable> {
	    protected FSContext fsContext;

	    public void setFSContext(FSContext fsContext) {
	      this.fsContext = fsContext;
	    }

	    /**
	     * This method is used when apps are tied in fairness ratio. It breaks
	     * the tie by submit time and job name to get a deterministic ordering,
	     * which is useful for unit tests.
	     *
	     * @param s1 the first item to compare
	     * @param s2 the second item to compare
	     * @return &lt; 0, 0, or &gt; 0 if the first item is less than, equal to,
	     * or greater than the second item, respectively
	     */
	    protected int compareAttribrutes(Schedulable s1, Schedulable s2) {
	      int res = (int) Math.signum(s1.getStartTime() - s2.getStartTime());

	      if (res == 0) {
	        res = s1.getName().compareTo(s2.getName());
	      }

	      return res;
	    }
	  }

	  /**
	   * This class compares two {@link Schedulable} instances according to the
	   * DRF policy in the special case that only CPU and memory are configured.
	   * If neither instance is below min share, approximate fair share
	   * ratios are compared.
	   */
	  @VisibleForTesting
	  static class TestComparator2
	      extends TestComparator {
		  
		//  temperature = calculateTemperature();

	    @Override
	    
	    public int compare(Schedulable s1, Schedulable s2) {
	 	   ResourceInformation[] usage1 =
			          s1.getResourceUsage().getResources();
		   ResourceInformation[] usage2 =
			          s2.getResourceUsage().getResources();
		   ResourceInformation[] request1 = s1.getDemand().getResources();
		   ResourceInformation[] request2 = s2.getDemand().getResources();
		   ResourceInformation[] minShareInfo1 = s1.getMinShare().getResources();
		   ResourceInformation[] minShareInfo2 = s2.getMinShare().getResources();
		   
		   
		   Resource clusterCapacity =
			          fsContext.getClusterResource();
		   Resource clusterUsage = 
				      fsContext.getClusterUsage();
		   Resource clusterAvailableResources =
			        Resources.subtract(clusterCapacity, clusterUsage);
		   
		   // shares[] = request/clustercapacity*weight
		   double[] shares1 = new double[2];
		   double[] shares2 = new double[2];
		   
		   
		   // dominant=0 => memory is dominant resource and dominant=1 => v_core is dominant resource
		   int dominant1 = calculateClusterAndFairRatios(request1,
			          s1.getWeight(), clusterCapacity.getResources(), shares1);
		   int dominant2 = calculateClusterAndFairRatios(request2,
			          s2.getWeight(), clusterCapacity.getResources(), shares2);
		   
		   boolean s1Needy = usage1[dominant1].getValue() <
			          minShareInfo1[dominant1].getValue();
		   boolean s2Needy = usage2[dominant2].getValue() <
			          minShareInfo2[dominant2].getValue();
		   
		   
		   // These arrays hold the fairness for needy time and not needy time
		   // ratios[0][x] are if not needy, ourFairness[] = request/cluster*weight 
	
		     
	       double[] ourFairness1 =new double [NUM_RESOURCES];
	       double[] ourFairness2 = new double[NUM_RESOURCES];
	       ourFairness1 = calculateOurFairness(usage1, s1.getWeight(), clusterCapacity.getResources());
	       
	       ourFairness2 = calculateOurFairness(usage2, s2.getWeight(), clusterCapacity.getResources());
	       
	       
	       
	       int fitness1 = calculateFitness(
	    		   request1, s1.getWeight(), clusterAvailableResources.getResources());
	       int fitness2 = calculateFitness(
	    		   request2, s2.getWeight(), clusterAvailableResources.getResources());
	      
	       int res = 0; 
	      
	       


	      if (!s2Needy && !s1Needy) {
	    	  
	    	  
	         res = compareSA( ourFairness1[dominant1], ourFairness2[dominant2]) ; 
	         	if(res==0) {
	         		res = compareAttribrutes(s1, s2);}  
	            
	      } else if (s1Needy && !s2Needy) {
	        res = -1;
	      } else if (s2Needy && !s1Needy) {
	        res = 1;
	      } else if (s1Needy && s2Needy) {
	        

	        res = (int) Math.signum(minShareInfo1[dominant1].getValue() - minShareInfo2[dominant2].getValue());

	        if (res == 0) {
	          res = (int) Math.signum(minShareInfo1[1 - dominant1].getValue() -
	              minShareInfo2[1 - dominant2].getValue());
	        }
	      }

	      if (res == 0) {
	        res = compareAttribrutes(s1, s2);
	      }

	      // adding schedulable to array of schedulables
	      if (res == -1) {
	     	 AddTOArray(s1);
	   	   
	      }
	      if (res == 1) {
	     	 AddTOArray(s2);
	   	   
	      }
	      
	      
	      return res;
	    }

	    /**
	     * Calculate a resource's usage ratio and approximate fair share ratio
	     * assuming that CPU and memory are the only configured resource types.
	     * The {@code shares} array will be populated with the approximate fair
	     * share ratio for each resource type. The approximate fair share ratio
	     * is calculated as {@code resourceInfo} divided by {@code cluster} and
	     * the {@code weight}. If the cluster's resources are 100MB and
	     * 10 vcores, the usage ({@code resourceInfo}) is 10 MB and 5 CPU, and the
	     * weights are 2, the fair share ratios will be 0.05 and 0.25.
	     *
	     * The approximate fair share ratio is the usage divided by the
	     * approximate fair share, i.e. the cluster resources times the weight.
	     * The approximate fair share is an acceptable proxy for the fair share
	     * because when comparing resources, the resource with the higher weight
	     * will be assigned by the scheduler a proportionally higher fair share.
	     *
	     * The length of the {@code shares} array must be at least 2.
	     *
	     * The return value will be the index of the dominant resource type in the
	     * {@code shares} array. The dominant resource is the resource type for
	     * which {@code resourceInfo} has the largest usage ratio.
	     *
	     * @param resourceInfo the resource for which to calculate ratios
	     * @param weight the resource weight
	     * @param clusterInfo the total cluster resources
	     * @param shares the share ratios array to populate
	     * @return the index of the resource type with the largest cluster share
	     */
	    @VisibleForTesting
	    int calculateClusterAndFairRatios(ResourceInformation[] resourceInfo,
	        float weight, ResourceInformation[] clusterInfo, double[] shares) {
	      int dominant;

	      shares[Resource.MEMORY_INDEX] =
	          ((double) resourceInfo[Resource.MEMORY_INDEX].getValue()) /
	          clusterInfo[Resource.MEMORY_INDEX].getValue();
	      shares[Resource.VCORES_INDEX] =
	          ((double) resourceInfo[Resource.VCORES_INDEX].getValue()) /
	          clusterInfo[Resource.VCORES_INDEX].getValue();
	      dominant =
	          shares[Resource.VCORES_INDEX] > shares[Resource.MEMORY_INDEX] ?
	          Resource.VCORES_INDEX : Resource.MEMORY_INDEX;

	      shares[Resource.MEMORY_INDEX] /= weight;
	      shares[Resource.VCORES_INDEX] /= weight;

	      return dominant;
	    }

	    /**
	     * Calculate a resource's min share ratios assuming that CPU and memory
	     * are the only configured resource types. The return array will be
	     * populated with the {@code resourceInfo} divided by {@code minShareInfo}
	     * for each resource type. If the min shares are 5 MB and 10 vcores, and
	     * the usage ({@code resourceInfo}) is 10 MB and 5 CPU, the ratios will
	     * be 2 and 0.5.
	     *
	     * The length of the {@code ratios} array must be 2.
	     *
	     * @param resourceInfo the resource for which to calculate min shares
	     * @param minShareInfo the min share
	     * @return the share ratios
	     */
	    @VisibleForTesting
	    double[] calculateMinShareRatios(ResourceInformation[] resourceInfo,
	        ResourceInformation[] minShareInfo) {
	      double[] minShares1 = new double[2];

	      // both are needy below min share
	      minShares1[Resource.MEMORY_INDEX] =
	          ((double) resourceInfo[Resource.MEMORY_INDEX].getValue()) /
	          minShareInfo[Resource.MEMORY_INDEX].getValue();
	      minShares1[Resource.VCORES_INDEX] =
	          ((double) resourceInfo[Resource.VCORES_INDEX].getValue()) /
	          minShareInfo[Resource.VCORES_INDEX].getValue();

	      return minShares1;
	    }
	    //**************************************************************************
	    @VisibleForTesting
	 
	    int calculateFitness(ResourceInformation[] request,
	        float weight, ResourceInformation[] clusterInfo) {
	 	  
	 	   
	      int fitness= ((int) request[Resource.MEMORY_INDEX].getValue()*
	     		 (int) clusterInfo[Resource.MEMORY_INDEX].getValue()) +
	     		 ((int) request[Resource.VCORES_INDEX].getValue()*
	     	    		 (int) clusterInfo[Resource.VCORES_INDEX].getValue()) ;

	     

	      return fitness;
	    }
	    
	    //**********************************************************************
	    
	    int compareSA( double ourfairness1, double ourfairness2) {
	 	   int ret = 0;
	 	   double minTemperature = 0 ;
	        double Alpha= 0.9 ;
	        double rand= Math.random();
	      
	 	   
	  {
	     	   
	     	   ret = -1; //initial state is S2
	     	   if( temperature > minTemperature ) {
	     		   
	     		   // simulated annealing starts
	         	   
	     	   if(ourfairness1 < ourfairness2){
	         	   ret = +1; // moves to next state
	         	  
	            }else if (ourfairness1 > ourfairness2){
	         	   
	         	   double p= Math.exp(-1*(ourfairness2-ourfairness1)/temperature) ;
	         	  LOG.info("probablity is : " + p + " random is : "
	         	          +rand);
	         	  
	         		   ret=  p > rand  ?
	         				   -1 : +1;
	         		   temperature*= Alpha ;
	         	   
	            
	         		  
	            }else if (ourfairness1 == ourfairness2){
	         	   ret = 0;
	         	   }
	            }
	      

	        }
	 	   return ret ;
	    }
	  //***********************************************************************
	      double [] calculateOurFairness(ResourceInformation[] resourceInfo,
		        float weight, ResourceInformation[] minshare) {
		      
		    double[] ourFairness =  new double[2];
			ourFairness[Resource.MEMORY_INDEX] =
		          ((double) resourceInfo[Resource.MEMORY_INDEX].getValue()) /
		          minshare[Resource.MEMORY_INDEX].getValue() * weight;
		    ourFairness[Resource.VCORES_INDEX] =
			      ((double) resourceInfo[Resource.VCORES_INDEX].getValue()) /
			      minshare[Resource.VCORES_INDEX].getValue() * weight;

		   return ourFairness;
	   }
	    //*********************************************************************************
	      void AddTOArray(Schedulable s) {
	    	  //if S does'nt exist
	 		if (! schedulables.contains(s)) {
	 			
	 			schedulables.add(s);
	 			Resource clusterCapacity =
	 			          fsContext.getClusterResource();
	 		   Resource clusterUsage = 
	 				      fsContext.getClusterUsage();
	 		   Resource clusterAvailableResources =
	 			        Resources.subtract(clusterCapacity, clusterUsage);
	 		   
	 			int fitness = calculateFitness(s.getDemand().getResources(),
	 					s.getWeight(), clusterAvailableResources.getResources());
	 			fitnessOfAll.add(fitness);
	 		}
	 		else {
	 			//// if S exists => update fitnessOfAll
	 			int index = schedulables.indexOf(s);
	 			Resource clusterCapacity =
	 			          fsContext.getClusterResource();
	 		   Resource clusterUsage = 
	 				      fsContext.getClusterUsage();
	 		   Resource clusterAvailableResources =
	 			        Resources.subtract(clusterCapacity, clusterUsage);
	 		   
	 			int fitness = calculateFitness(s.getDemand().getResources(),
	 					s.getWeight(), clusterAvailableResources.getResources());
	 			fitnessOfAll.set(index, fitness);
	 		}
	 		
	 	
	 	}
	
	     //***********************************************************************************
	  }
	  
	  
}