package ed.inf.adbs.lightdb;

import ed.inf.adbs.lightdb.Operator.JoinOperator;
import ed.inf.adbs.lightdb.Operator.Operator;
import ed.inf.adbs.lightdb.Utils.DatabaseCatalog;
import ed.inf.adbs.lightdb.Utils.Tuple;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.function.Function;
import java.util.stream.Stream;


/**
 * Lightweight in-memory database system
 */
public class LightDB {

    public static void main(String[] args) {

//        String currentPath = System.getProperty("user.dir");
//        String databaseDir = args.length >= 3 ? currentPath + args[0] : currentPath + "/samples/db";
//        String inputFile = args.length >= 3 ? currentPath + args[1] : currentPath + "/samples/input/query11.sql";
//        String outputFile = args.length >= 3 ? currentPath + args[2] : currentPath + "/samples/test_output/output11.csv";


        if (args.length != 3) {
            System.err.println("Usage: LightDB database_dir input_file output_file");
            return;
        }
        String databaseDir = args[0];
        String inputFile = args[1];
        String outputFile = args[2];


        System.out.println("databaseDir = " + databaseDir);
        System.out.println("inputFile = " + inputFile);
        System.out.println("outputFile = " + outputFile);

        DatabaseCatalog.getsInstance().setDatabaseDir(databaseDir);


        Planner planner = new Planner(inputFile, outputFile);
        Operator operator = planner.createPlan();


        if (operator != null) {
            System.out.println("-----------------output-------start--------------------");
            Tuple tuple;
            while ((tuple = operator.getNextTuple()) != null) {
                System.out.println("tuple = " + tuple);
                try {
                    operator.dump(tuple);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            System.out.println("-----------------output-------end--------------------");

            System.out.println("JoinOperator.outerLoopCount = " + JoinOperator.outerLoopCount);
            System.out.println("JoinOperator.innerLoopCount = " + JoinOperator.innerLoopCount);

        }


    }
}
 /*File inputDir = new File(currentPath + "/samples/input");

        Stream<String> stringStream = Arrays.stream(inputDir.listFiles()).map(new Function<File, String>() {
            @Override
            public String apply(File file) {
                return file.getAbsolutePath();
            }
        });

        Stream<String> sorted = stringStream.sorted();

        sorted.forEach(new Stream.Builder<String>() {
            @Override
            public void accept(String input) {
                System.out.println("1111111111111111==========" + input);

                // E:\work\CXC\CW1\LightDB2\samples\input\query01.sql
                // /samples/test_output/query01.csv
                int indexOf = input.lastIndexOf("\\");
                String substring = input.substring(indexOf);

                System.out.println("22222222222==========" + substring);

                File output = new File(currentPath + "/samples/test_output/" + substring.split("\\.")[0] + ".csv");


                System.out.println("currentPath = " + currentPath);
                System.out.println("databaseDir = " + databaseDir);
                System.out.println("inputFile = " + input);
                System.out.println("outputFile = " + output.getAbsolutePath());

                DatabaseCatalog.getsInstance().setDatabaseDir(databaseDir);


                Planner planner = new Planner(input, output.getAbsolutePath());
                Operator operator = planner.createPlan();


                if (operator != null) {
                    System.out.println("-----------------output-------start--------------------");
                    Tuple tuple;
                    while ((tuple = operator.getNextTuple()) != null) {
                        System.out.println("tuple = " + tuple);
                        try {
                            operator.dump(tuple);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }
                    System.out.println("-----------------output-------end--------------------");

                    System.out.println("JoinOperator.outerLoopCount = " + JoinOperator.outerLoopCount);
                    System.out.println("JoinOperator.innerLoopCount = " + JoinOperator.innerLoopCount);

                }


            }

            @Override
            public Stream<String> build() {
                return null;
            }
        });*/