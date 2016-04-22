/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package iterator;

import bufmgr.PageNotReadException;
import global.AttrOperator;
import global.AttrType;
import global.GlobalConst;
import global.RID;
import global.TupleOrder;
import heap.Heapfile;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import heap.Scan;
import heap.Tuple;
import index.IndexException;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author tmatulat
 */
public class PermutationJoins extends Iterator implements GlobalConst {

    private AttrType _in1[], _in2[];
    private int in1_len, in2_len;
    private Iterator outer, p_i1, // pointers to the two iterators. If the
            p_i2, p_i12, p_i22;                   // inputs are sorted, then no sorting is done
    private short t2_str_sizescopy[];
    private CondExpr OutputFilter[];
    private CondExpr RightFilter[];
    private short inner_str_sizes[];  // size of string
    private int n_buf_pgs;        // # of buffer pages available.
    private boolean done, // Is the join complete
            get_from_outer;                 // if TRUE, a tuple is got from outer
    private Tuple outer_tuple, inner_tuple;
    private Tuple Jtuple;           // Joined tuple
    private FldSpec perm_mat[];
    private int nOutFlds;
    private Heapfile hf;
    private Scan inner;

    public PermutationJoins(AttrType in1[],
            int len_in1,
            short s1_sizes[],
            AttrType in2[],
            int len_in2,
            short s2_sizes[],
            int join_col_in1,
            int sortFld1Len,
            int join_col_in2,
            int sortFld2Len,
            int join_col_in12,
            int join_col_in22,            
            
            int amt_of_mem,
            Iterator am1,
            Iterator am2,
            Iterator am3,
            Iterator am4,
            boolean in1_sorted,
            boolean in2_sorted,
            CondExpr outFilter[],
            FldSpec proj_list[],
            int n_out_flds) throws JoinNewFailed, JoinLowMemory, SortException, TupleUtilsException, IOException {

        _in1 = new AttrType[in1.length];
        _in2 = new AttrType[in2.length];
        System.arraycopy(in1, 0, _in1, 0, in1.length);
        System.arraycopy(in2, 0, _in2, 0, in2.length);
        in1_len = len_in1;
        in2_len = len_in2;

        t2_str_sizescopy = s2_sizes;
        inner_tuple =new Tuple();
        outer_tuple = new Tuple();
        Jtuple = new Tuple();
        perm_mat = proj_list;
        nOutFlds = n_out_flds;
        AttrType[] Jtypes = new AttrType[n_out_flds];
        short[] ts_size;
               
        try {
            ts_size = TupleUtils.setup_op_tuple(Jtuple, Jtypes,
                    in1, len_in1, in2, len_in2,
                    s1_sizes, s2_sizes,
                    proj_list, n_out_flds);
        } catch (Exception e) {
            throw new TupleUtilsException(e, "Exception is caught by PermutationJoins.java");
        }        
        
        // SORTING on List
        int n_strs2 = 0;
     
        for (int i = 0; i < len_in2; i++) {
            if (_in2[i].attrType == AttrType.attrString) {
                n_strs2++;
            }
        }
        inner_str_sizes = new short[n_strs2];

        for (int i = 0; i < n_strs2; i++) {
            inner_str_sizes[i] = s2_sizes[i];
        }
         
        p_i1 = am1;
        p_i12 = am3;
        p_i2 = am2;
        p_i22 = am4;                

        TupleOrder order1 = new TupleOrder(TupleOrder.Ascending);
  
        if (outFilter[0].op.attrOperator == AttrOperator.aopGT || outFilter[0].op.attrOperator == AttrOperator.aopLE) {

            order1 = new TupleOrder(TupleOrder.Descending);
        }     
        
        TupleOrder order2 = new TupleOrder(TupleOrder.Descending);
        
        if (outFilter[1].op.attrOperator == AttrOperator.aopGT || outFilter[1].op.attrOperator == AttrOperator.aopLE) {
            order2 = new TupleOrder(TupleOrder.Ascending);
        }
        
        AttrType[] jtype2 = { new AttrType( AttrType.attrInteger), 
            new AttrType( AttrType.attrInteger), new AttrType( AttrType.attrInteger),
            new AttrType( AttrType.attrInteger) }; 
        Tuple t = new Tuple();
        
        if (!in1_sorted) {
            try {
                p_i1 = new Sort(in1, (short) len_in1, s1_sizes, am1, join_col_in1,
                        order1, sortFld1Len, amt_of_mem / 2);         
                
                p_i12 = new Sort(in1, (short) len_in1, s1_sizes, am3, join_col_in12,
                        order1, sortFld1Len, amt_of_mem / 2);
            } catch (Exception e) {
                throw new SortException(e, "Sort failed");
            }
        }        

/*
        java.util.ArrayList<Tuple> L1 = new java.util.ArrayList();
        try {
            while ((t = p_i12.get_next()) != null) {
                //t.print(jtype2);
                L1.add( t );
            }
        } catch (Exception e) {
            System.err.println("something going wrong : " + e);
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }    
 */       
        
        if (!in2_sorted) {
            try {
                p_i2 = new Sort(in2, (short) len_in2, s2_sizes, am2, join_col_in2,
                        order2, sortFld2Len, amt_of_mem / 2);
                p_i22 = new Sort(in2, (short) len_in2, s2_sizes, am2, join_col_in22,
                        order2, sortFld2Len, amt_of_mem / 2);                
            } catch (Exception e) {
                throw new SortException(e, "Sort L1' failed");
            }
        }

        
        System.out.println("Check the content condition  p_i12:");                
        try {
            while ((t = p_i12.get_next()) != null) {
                t.print(jtype2);
            }
        } catch (Exception e) {
            System.err.println("something going wrong : " + e);
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }       

        
        System.out.println("Check the content condition  p_i1:");
        try {
            while ((t = p_i1.get_next()) != null) {
                t.print(jtype2);
            }
        } catch (Exception e) {
            System.err.println("" + e);
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }
        
        
    
//        System.out.println("Array List L1 ");
//        java.util.Iterator it = L1.iterator();
//        while (it.hasNext()){
//            ((Tuple) it.next()).print(jtype2);
//        }
        
        
        
        System.out.println("Check the content condition  p_i2:");
        AttrType[] jtype3 = { new AttrType( AttrType.attrInteger), 
            new AttrType( AttrType.attrInteger), new AttrType( AttrType.attrInteger),
            new AttrType( AttrType.attrInteger) };            
        try {
            while ((t = p_i2.get_next()) != null) {
                t.print(jtype2);

            }
        } catch (Exception e) {
            System.err.println("" + e);
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }

        System.out.println("Check the content condition  p_i22:");
        AttrType[] jtype32 = { new AttrType( AttrType.attrInteger), 
            new AttrType( AttrType.attrInteger), new AttrType( AttrType.attrInteger),
            new AttrType( AttrType.attrInteger) };            
        try {
            while ((t = p_i22.get_next()) != null) {
                t.print(jtype32);

            }
        } catch (Exception e) {
            System.err.println("" + e);
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }
        
        
        

    }

    @Override
    public Tuple get_next()
            throws IOException,
            JoinsException,
            IndexException,
            InvalidTupleSizeException,
            InvalidTypeException,
            PageNotReadException,
            TupleUtilsException,
            PredEvalException,
            SortException,
            LowMemException,
            UnknowAttrType,
            UnknownKeyTypeException,
            Exception {
      
      if (done)
	return null;
      
      do
	{
	  // If get_from_outer is true, Get a tuple from the outer, delete
	  // an existing scan on the file, and reopen a new scan on the file.
	  // If a get_next on the outer returns DONE?, then the nested loops
	  //join is done too.
	  
	  if (get_from_outer == true)
	    {
	      get_from_outer = false;
	      if (inner != null)     // If this not the first time,
		{
		  // close scan
		  inner = null;
		}
	    
	      try {
		inner = hf.openScan();
	      }
	      catch(Exception e){
		throw new NestedLoopException(e, "openScan failed");
	      }
	      
	      if ((outer_tuple=outer.get_next()) == null)
		{
		  done = true;
		  if (inner != null) 
		    {
                      
		      inner = null;
		    }
		  
		  return null;
		}   
	    }  // ENDS: if (get_from_outer == TRUE)
	 
	  
	  // The next step is to get a tuple from the inner,
	  // while the inner is not completely scanned && there
	  // is no match (with pred),get a tuple from the inner.
	  
	 
	      RID rid = new RID();
	      while ((inner_tuple = inner.getNext(rid)) != null)
		{
		  inner_tuple.setHdr((short)in2_len, _in2,t2_str_sizescopy);
		  if (PredEval.Eval(RightFilter, inner_tuple, null, _in2, null) == true)
		    {
		      if (PredEval.Eval(OutputFilter, outer_tuple, inner_tuple, _in1, _in2) == true)
			{
			  // Apply a projection on the outer and inner tuples.
			  Projection.Join(outer_tuple, _in1, 
					  inner_tuple, _in2, 
					  Jtuple, perm_mat, nOutFlds);
			  return Jtuple;
			}
		    }
		}
	      
	      // There has been no match. (otherwise, we would have 
	      //returned from t//he while loop. Hence, inner is 
	      //exhausted, => set get_from_outer = TRUE, go to top of loop
	      
	      get_from_outer = true; // Loop back to top and get next outer tuple.	      
	} while (true);    }

    @Override
    public void close()
            throws IOException,
            JoinsException,
            SortException,
            IndexException {
        if (!closeFlag) {

            try {
                outer.close();
            } catch (Exception e) {
                throw new JoinsException(e, "NestedLoopsJoin.java: error in closing iterator.");
            }
            closeFlag = true;
        }
    }
}
