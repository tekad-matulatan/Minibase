/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package tests;

import console.CommandProcessor;
import console.ParserResult;
import console.QueryDriver;
import console.SetupDB;

/**
 *
 * @author tmatulat
 */
public class Phase4 {
    
    
    public static void main(String[] args){
        new SetupDB();

        CommandProcessor obj = new CommandProcessor();
        ParserResult pr = null;
        try {
            while (true) {
                System.out.print("Input<" + QueryDriver.defaultSchema + ">:");
                pr = obj.input();

                for (String columns : pr.getProjection()) {
                    System.out.println("Columns :" + columns);
                }
                for (String tab : pr.getTables()) {
                    System.out.println(" Table :" + tab);
                }
                for (String columns : pr.getPredicates()) {
                    System.out.println("predicates :" + columns);
                }

                for (String columns : pr.getOperator()) {
                    System.out.println("operator :" + columns);
                }

                for (String columns : pr.getJoinOperator()) {
                    System.out.println("Join operator :" + columns);
                }
                
                obj.processQuery(pr);
            }
        } catch (exception.QueryException ex) {
            System.out.println(ex);
        }

        
    }
}
