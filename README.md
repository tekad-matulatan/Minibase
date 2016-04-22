# Minibase
Minibase db enhancements and additional functions under DBMSI Coursework
NEW VERSION -------

I have update the library
It is consists of package
- console
-   CommandProcessor
-   ParserResult
-   QueryDriver
-   QueryParser
-   QueryValidator
-   Session
-   SetupDB
-   Table
- exception
-   QueryException
- iterator <---- this one from original minibase
-   PermutationJoin <--- this my version from the paper IEJOIN
-   

How to use it...
it is better you delete the whole iterator folder from source because it is already inside the library
the data I use is still the old data from phase 3 with modification.. look inside the data.zip
you should put new folder "data" in the same folder with your javaminibase



then in your main method
    public static void main(String[] args) {
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



