package io.farragolabs.pig;

public class Query {

    public static final String header = "SET pig.exec.mapPartAgg true;\n" +
            "SET default_parallel 10;\n" +
            "SET pig.pretty.print.schema true;\n" +
            "\n";

    public static final String delta =
            "DEFINE load_hbase_increment (table_name, start_time, last_time, content, datatypes) returns Z {\n" +
                    "\n" +
                    "hbs0 = LOAD 'hbase://$table_name'\n" +
                    "        USING org.apache.pig.backend.hadoop.hbase.HBaseStorage(\n" +
                    "               '$content','-loadKey true -gte 0-$start_time -lt 0-$last_time')\n" +
                    "\t\t\t   AS ( id:bytearray, $datatypes);\n" +
                    "\n" +
                    "hbs1 = LOAD 'hbase://$table_name'\n" +
                    "        USING org.apache.pig.backend.hadoop.hbase.HBaseStorage(\n" +
                    "               '$content','-loadKey true -gte 1-$start_time -lt 1-$last_time')\n" +
                    "\t\t\t   AS ( id:bytearray, $datatypes);\n" +
                    "\n" +
                    "hbs2 = LOAD 'hbase://$table_name'\n" +
                    "        USING org.apache.pig.backend.hadoop.hbase.HBaseStorage(\n" +
                    "               '$content','-loadKey true -gte 2-$start_time -lt 2-$last_time')\n" +
                    "\t\t\t   AS ( id:bytearray, $datatypes);\n" +
                    "\n" +
                    "hbs3 = LOAD 'hbase://$table_name'\n" +
                    "        USING org.apache.pig.backend.hadoop.hbase.HBaseStorage(\n" +
                    "               '$content','-loadKey true -gte 3-$start_time -lt 3-$last_time')\n" +
                    "\t\t\t   AS ( id:bytearray, $datatypes);\n" +
                    "\n" +
                    "hbs4 = LOAD 'hbase://$table_name'\n" +
                    "        USING org.apache.pig.backend.hadoop.hbase.HBaseStorage(\n" +
                    "               '$content','-loadKey true -gte 4-$start_time -lt 4-$last_time')\n" +
                    "\t\t\t   AS ( id:bytearray, $datatypes);\n" +
                    "\n" +
                    "hbs5 = LOAD 'hbase://$table_name'\n" +
                    "        USING org.apache.pig.backend.hadoop.hbase.HBaseStorage(\n" +
                    "               '$content','-loadKey true -gte 5-$start_time -lt 5-$last_time')\n" +
                    "\t\t\t   AS ( id:bytearray, $datatypes);\n" +
                    "\n" +
                    "hbs6 = LOAD 'hbase://$table_name'\n" +
                    "        USING org.apache.pig.backend.hadoop.hbase.HBaseStorage(\n" +
                    "               '$content','-loadKey true -gte 6-$start_time -lt 6-$last_time')\n" +
                    "\t\t\t   AS ( id:bytearray, $datatypes);\n" +
                    "\n" +
                    "hbs7 = LOAD 'hbase://$table_name'\n" +
                    "        USING org.apache.pig.backend.hadoop.hbase.HBaseStorage(\n" +
                    "               '$content','-loadKey true -gte 7-$start_time -lt 7-$last_time')\n" +
                    "\t\t\t   AS ( id:bytearray, $datatypes);\n" +
                    "\n" +
                    "hbs8 = LOAD 'hbase://$table_name'\n" +
                    "        USING org.apache.pig.backend.hadoop.hbase.HBaseStorage(\n" +
                    "               '$content','-loadKey true -gte 8-$start_time -lt 8-$last_time')\n" +
                    "\t\t\t   AS ( id:bytearray, $datatypes);\n" +
                    "\n" +
                    "hbs9 = LOAD 'hbase://$table_name'\n" +
                    "        USING org.apache.pig.backend.hadoop.hbase.HBaseStorage(\n" +
                    "               '$content','-loadKey true -gte 9-$start_time -lt 9-$last_time')\n" +
                    "\t\t\t   AS ( id:bytearray, $datatypes);\n" +
                    "\n" +
                    "\n" +
                    "$Z = union hbs0, hbs1, hbs2, hbs3, hbs4, hbs5, hbs6, hbs7, hbs8, hbs9;\n" +
                    "\n" +
                    "} ;\n";

      public static String load_delta = "load_hbase_increment ('%s', '%s', '%s','%s','%s');";

    public static String load = "LOAD 'hbase://%s' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage('%s') " +
            "AS ( id:bytearray, %s);";
}
