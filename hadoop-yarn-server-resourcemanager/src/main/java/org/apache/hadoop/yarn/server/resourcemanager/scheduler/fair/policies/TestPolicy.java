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
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.Schedulable;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.SchedulingPolicy;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.policies.DominantResourceFairnessPolicy.DominantResourceFairnessComparator;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.policies.DominantResourceFairnessPolicy.DominantResourceFairnessComparator2;
import org.apache.hadoop.yarn.util.resource.DominantResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;
public class TestPolicy extends SchedulingPolicy {
   
	public static final String NAME = "TEST";
	private static final int NUM_RESOURCES =
		      ResourceUtils.getNumberOfKnownResourceTypes();
	private static final TestComparator2 COMPARATOR =
		      new TestComparator2();
	private static final DominantResourceCalculator CALCULATOR =
		      new DominantResourceCalculator();
	private static final ArrayList<Schedulable>  schedulables = null ;
	private static final ArrayList<Double>  NoneNeedyfairnessOfAll = null ; 
	private static final ArrayList<Double>  NeedyfairnessOfAll = null ; 
	private static final int NeedyIndex = 1 ;
	private static final int NotNeedyIndex = 0 ;
	
	
	//temperature[1] when it's needy && temperature[0] when it's not needy
	
	@Override
	public ResourceCalculator getResourceCalculator() {
		// TODO Auto-generated method stub
	return CALCULATOR;
		
	}

	@Override
	public String getName() {
		// TODO Auto-generated method stub
		return NAME;
	}

	@Override
	public Comparator<Schedulable> getComparator() {
		// TODO Auto-generated method stub
		return COMPARATOR;
	}

	@Override
	public void computeShares(Collection<? extends Schedulable> schedulables, Resource totalResources) {
		// TODO Auto-generated method stub
		for (ResourceInformation info: ResourceUtils.getResourceTypesArray()) {
		      ComputeFairShares.computeShares(schedulables, totalResources,
		          info.getName());
		    }
	}

	@Override
	public void computeSteadyShares(Collection<? extends FSQueue> queues, Resource totalResources) {
		// TODO Auto-generated method stub
		 for (ResourceInformation info: ResourceUtils.getResourceTypesArray()) {
		      ComputeFairShares.computeSteadyShares(queues, totalResources,
		          info.getName());
		    }
		
	}

	@Override
	public boolean checkIfUsageOverFairShare(Resource usage, Resource fairShare) {
		// TODO Auto-generated method stub
		return !Resources.fitsIn(usage, fairShare);
	}

	@Override
	public Resource getHeadroom(Resource queueFairShare, Resource queueUsage, Resource maxAvailable) {
		// TODO Auto-generated method stub
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
	
	
	static double calculateTemperature(int Needy) {
		 double temperature ;
		 if (Needy == NeedyIndex) {
	     double max = Collections.max(NeedyfairnessOfAll);
	     double min = Collections.min(NeedyfairnessOfAll);
	     double entropy = max - min ;
	      temperature = entropy * 1000 ;
		 }else {
			 double max = Collections.max(NoneNeedyfairnessOfAll);
		     double min = Collections.min(NoneNeedyfairnessOfAll);
		     double entropy = max - min ;
		      temperature = entropy * 1000 ;
		 }
	   return temperature;
}
	
	
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

	
	 static class TestComparator2
     extends TestComparator {
		 
		 private static double[] temperature = {calculateTemperature(NotNeedyIndex) ,
					calculateTemperature(NeedyIndex)} ;
   @Override
   public int compare(Schedulable s1, Schedulable s2) {
	   
	   
	   ResourceInformation[] usage1 =
		          s1.getResourceUsage().getResources();
	   ResourceInformation[] uage2 =
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
	   boolean s2Needy = uage2[dominant2].getValue() <
		          minShareInfo2[dominant2].getValue();
	   
	   
	   // These arrays hold the fairness for needy time and not needy time
	   // ratios[0][x] are if not needy, ourFairness[] = request/cluster*weight 
	   // ratios[1][x] are needy, ourFairness[] = request/minshares*weight
	     
       double[][] ourFairness1 =new double [2][NUM_RESOURCES];
       double[][] ourFairness2 = new double [2][NUM_RESOURCES];
       ourFairness1[0] = calculateOurFairness(usage1, s1.getWeight(), clusterCapacity.getResources());
       ourFairness1[1] = calculateOurFairness(uage2, s2.getWeight(), minShareInfo1);
       ourFairness2[0] = calculateOurFairness(usage1, s1.getWeight(), clusterCapacity.getResources());
       ourFairness2[1] = calculateOurFairness(uage2, s2.getWeight(), minShareInfo2);
       
       
       int fitness1 = calculateFitness(
    		   request1, s1.getWeight(), clusterAvailableResources.getResources());
       int fitness2 = calculateFitness(
    		   request2, s2.getWeight(), clusterAvailableResources.getResources());
       int res = 0; 
      
       

       if (!s2Needy && !s1Needy) {
    	  
         res = compareSA( fitness1, fitness2, NotNeedyIndex) ; 
         	if(res==0) {
         		res = compareAttribrutes(s1, s2);
         	}

       } else if (s1Needy && !s2Needy) {
    	   
         res = -1;
         
       } else if (s2Needy && !s1Needy) {
    	   
         res = 1;
         
       } else if (s2Needy && s1Needy) {
    	  
    	   
    	   res = compareSA( fitness1, fitness2, NeedyIndex) ;
    	   if (res == 0){
        	   res = compareAttribrutes(s1, s2);
    	   }

       }
     
     if (res == -1) {
    	 AddTOArray(s1 , dominant1);
  	   
     }
     if (res == 1) {
    	 AddTOArray(s2 , dominant2);
  	   
     }
       
       return res;   
       
       
   }
 


//***********************************************************************      
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
    //************************************************************************
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
       float weight, ResourceInformation[] clusterAvailableResources) {
	  
	   
     int fitness= ((int) request[Resource.MEMORY_INDEX].getValue()*
    		 (int) clusterAvailableResources[Resource.MEMORY_INDEX].getValue()) +
    		 ((int) request[Resource.VCORES_INDEX].getValue()*
    	    		 (int) clusterAvailableResources[Resource.VCORES_INDEX].getValue()) ;

    

     return fitness;
   }
   
   //**********************************************************************
   int compareFitness(float fitness1 , float fitness2) {
	   int ret = 0;
	   
	   if (fitness1 > fitness2) {
		   ret = 1;
	   }else if (fitness1 < fitness2) {
		   ret = -1 ;
	   }
	   
	   return ret ;
   }
   
 //**********************************************************************
   
   int compareSA( float fitness1, float fitness2 , int ISNeedy) {
	   int ret = 0;
	   double minTemperature = 10 ;
	 
       double Alpha= 0.9 ;
       double rand= Math.random(); 
       
	   
 {
    	   
    	   ret = -1; //initial state is S1
    	   if( temperature[ISNeedy] > minTemperature ) {
    		   
    		   // simulated annealing starts
        	   
    	   if(fitness1 < fitness2){
        	   ret = 1; // moves to next state
        	  
           }else if (fitness1 > fitness2){
        	   
        	   double p= Math.exp((fitness2-fitness1)/ temperature[ISNeedy]) ;
        	   
        		   ret=  p > rand  ?
        				   1 : -1;
        		   temperature[ISNeedy]*= Alpha ;
        	   
           
        		  
           }else if (fitness1 == fitness2){
        	   ret = 0;
        	   }
           }
     

       }
	   return ret ;
   }
	//***********************************************************************
      double [] calculateOurFairness(ResourceInformation[] usage,
	        float weight, ResourceInformation[] minshare) {
	      
	    double[] ourFairness = null;
		ourFairness[Resource.MEMORY_INDEX] =
	          ((double) usage[Resource.MEMORY_INDEX].getValue()) /
	          minshare[Resource.MEMORY_INDEX].getValue() * weight;
	    ourFairness[Resource.VCORES_INDEX] =
		      ((double) usage[Resource.VCORES_INDEX].getValue()) /
		      minshare[Resource.VCORES_INDEX].getValue() * weight;

	   return ourFairness;
   }
   //*****************************************************************************
  

//*********************************************************************************
     // adds  value of ourfairness of dominant type to fairnessOfAll
     // adds schedulables Array
      void AddTOArray(Schedulable s , int dominant) {
    	  //if S does'nt exist
 		if (! schedulables.contains(s)) {
 			
 	
 			double[] Needyfairness = calculateOurFairness(s.getResourceUsage().getResources(),
 					s.getWeight(), s.getMinShare().getResources());
 			NeedyfairnessOfAll.add(Needyfairness[dominant]);
 			
 			Resource clusterCapacity =
			          fsContext.getClusterResource();
 			
 			double[] NoneNeedyfairness = calculateOurFairness(s.getResourceUsage().getResources(),
 					s.getWeight(), clusterCapacity.getResources());
 			NoneNeedyfairnessOfAll.add(NoneNeedyfairness[dominant]);
 		}
 		// if S exists => update NeedyfairnessOfAll and NoneNeedyfairnessOfAll
 		else {
 			int index = schedulables.indexOf(s);
 			double[] Needyfairness = calculateOurFairness(s.getResourceUsage().getResources(),
 					s.getWeight(), s.getMinShare().getResources());
 			NeedyfairnessOfAll.set(index, Needyfairness[dominant]);
 			
 			Resource clusterCapacity =
			          fsContext.getClusterResource();
 			
 			double[] NoneNeedyfairness = calculateOurFairness(s.getResourceUsage().getResources(),
 					s.getWeight(), clusterCapacity.getResources());
 			NoneNeedyfairnessOfAll.set(index, NoneNeedyfairness[dominant]);
 		}
 	}
//*********************************************************************************
}	 
}