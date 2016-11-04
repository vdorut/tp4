import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.fs.*;

public class SocialBFF {
    private static Configuration conf = null;
    /**
     * Initialization
     */
    static {
        conf = HBaseConfiguration.create();
        conf.addResource(new Path("/etc/hbase/conf/hbase-default.xml"));
        conf.addResource(new Path("/etc/hbase/conf/hbase-site.xml"));
    }

    /**
     * Create a table
     */
    public static void creatTable(String tableName, String[] familys)
            throws Exception {
        HBaseAdmin admin = new HBaseAdmin(conf);
        if (admin.tableExists(tableName)) {
            System.out.println("table already exists!");
        } else {
            HTableDescriptor tableDesc = new HTableDescriptor(tableName);
            for (int i = 0; i < familys.length; i++) {
                tableDesc.addFamily(new HColumnDescriptor(familys[i]));
            }
            admin.createTable(tableDesc);
            System.out.println("create table " + tableName + " ok.");
        }
    }

    /**
     * Delete a table
     */
    public static void deleteTable(String tableName) throws Exception {
        try {
            HBaseAdmin admin = new HBaseAdmin(conf);
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
            System.out.println("delete table " + tableName + " ok.");
        } catch (MasterNotRunningException e) {
            e.printStackTrace();
        } catch (ZooKeeperConnectionException e) {
            e.printStackTrace();
        }
    }

    /**
     * Put (or insert) a row
     */
    public static void addRecord(String tableName, String rowKey,
                                 String family, String qualifier, String value) throws Exception {
        try {
            HTable table = new HTable(conf, tableName);
            Put put = new Put(Bytes.toBytes(rowKey));
            put.add(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes
                    .toBytes(value));
            table.put(put);
            System.out.println("insert recored " + rowKey + " to table "
                    + tableName + " ok.");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Delete a row
     */
    public static void delRecord(String tableName, String rowKey)
            throws IOException {
    HTable table = new HTable(conf, tableName);
    List<Delete> list = new ArrayList<Delete>();
    Delete del = new Delete(rowKey.getBytes());
        list.add(del);
        table.delete(list);
        System.out.println("del recored " + rowKey + " ok.");
}

    /**
     * Get a row
     */
    public static void getOneRecord (String tableName, String rowKey) throws IOException{
        HTable table = new HTable(conf, tableName);
        Get get = new Get(rowKey.getBytes());
        Result rs = table.get(get);
        for(KeyValue kv : rs.raw()){
            System.out.print(new String(kv.getRow()) + " " );
            System.out.print(new String(kv.getFamily()) + ":" );
            System.out.print(new String(kv.getQualifier()) + " " );
            System.out.print(kv.getTimestamp() + " " );
            System.out.println(new String(kv.getValue()));
        }
    }
    /**
     * Scan (or list) a table
     */
    public static void getAllRecord (String tableName) {
        try{
            HTable table = new HTable(conf, tableName);
            Scan s = new Scan();
            ResultScanner ss = table.getScanner(s);
            for(Result r:ss){
                for(KeyValue kv : r.raw()){
                    System.out.print(new String(kv.getRow()) + " ");
                    System.out.print(new String(kv.getFamily()) + ":");
                    System.out.print(new String(kv.getQualifier()) + " ");
                    System.out.print(kv.getTimestamp() + " ");
                    System.out.println(new String(kv.getValue()));
                }
            }
        } catch (IOException e){
            e.printStackTrace();
        }
    }

    public static void addUser(String tableName) throws Exception{
        Scanner scan = new Scanner(System.in);
        String name, gender, bff, age, mail, address;
        String[] friendsTab;
        bff=null;

        System.out.println("Please select the name of the new user :");
        name = scan.nextLine();
        System.out.println("What is " + name + "'s gender ? ");
        gender = scan.nextLine();
        System.out.println("What is " + name + "'s age ? ");
        age = scan.nextLine();
        System.out.println("What is " + name + "'s mail ? ");
        mail = scan.nextLine();
        System.out.println("What is " + name + "'s address ? ");
        address = scan.nextLine();
        while(bff==null){
            System.out.println("Who is " + name + "'s BFF ? ");
            bff = scan.nextLine();
            if(bff==null) System.out.println("The value is mandatory, you can't live without a BFF");
        }

        System.out.println("Who is " + name + "'s other friends ? (type them separated by ',') ");
        friendsTab = scan.nextLine().split(",");

        SocialBFF.addRecord(tableName, name, "info", "age", age);
        SocialBFF.addRecord(tableName, name, "info", "gender", gender);
        SocialBFF.addRecord(tableName, name, "info", "mail", mail);
        SocialBFF.addRecord(tableName, name, "info", "address", address);
        SocialBFF.addRecord(tableName, name, "friends", "bff", bff);
        for(int i=0; i<friendsTab.length; i++) {
            SocialBFF.addRecord(tableName, name, "friends", "others", friendsTab[i]);
        }

        System.out.println("Thank you, welcome on Social BFF !");

    }

    public static void printHelp(){
        System.out.println("0 - help");
        System.out.println("1 - show the database");
        System.out.println("2 - add a new user");
        System.out.println("3 - check consistency between friends");
        System.out.println("4 - A cute random french sentence on friendship");
        System.out.println("5 - Exit Social BFF");
    }

    public static void swaggyPhrase(){
        String[] phrases = {"Je suis riche de mes amis, car ils sont en or (SkyBlog)",
                "La vie c'est dur quand t'es seul et mal-aimé, et l'amitié est une richesse qui à elle seule m'a aidé.(Kery James)",
                "La seule façon d'avoir un ami est d'en être un.(SkyBlog)",
                "Un ami est celui qui vous laisse l'entière liberté d'être vous-même.(Jim Morrison)",
                "Les vrais amis t’aiment pour ce que tu es et non pour ce qu'ils veulent que tu sois.(Ted Rall)",
                "Ce qui rend les amitiés indissolubles et double leur charme est un sentiment qui manque à l'amour : la certitude.(Honoré de Balzac)",
                "Un ami... rien n'est plus commun que le nom, rien n'est plus rare que la chose.(Jean de La Fontaine - Paroles de Socrate)"};
        double index = Math.random() * phrases.length;
        String maphrase = phrases[(int) index];
        System.out.println(maphrase);
    }

    public static void checkConsistency(){
        System.out.println("Not yet implemented...");
    }

    public static void main(String[] args) {
        Scanner scan = new Scanner(System.in);
        String saisieUtilisateur;
        boolean boucle = true;
        System.out.println("##### Connection...#####");
        try {


            //This database must contain these informations:
            String dataBase = "socialBFF";
            //as row id: the firstnames of peoples
            //as CFs: info, friends
            String[] columnFamily = { "info", "friends" };
            SocialBFF.creatTable(dataBase, columnFamily);

            System.out.println("##### Social Best Friend Forever ! #####");
            swaggyPhrase();
            printHelp();

            //Read Eval Print Loop (REPL)
            while(boucle==true){
                System.out.print("# ");
                saisieUtilisateur = scan.nextLine();

                if(saisieUtilisateur.equals("0")){
                    printHelp();
                }else if(saisieUtilisateur.equals("1")){
                    SocialBFF.getAllRecord(dataBase);
                }else if(saisieUtilisateur.equals("2")){
                    SocialBFF.addUser(dataBase);
                }else if(saisieUtilisateur.equals("3")){
                    SocialBFF.checkConsistency();
                }else if(saisieUtilisateur.equals("4")){
                    SocialBFF.swaggyPhrase();
                }else if(saisieUtilisateur.equals("5")){
                    System.out.println("Have a nice day !");
                    boucle = false;
                }
                else System.out.println("Invalid Input, please try again"); ;

            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}