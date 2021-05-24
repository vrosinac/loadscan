/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package loadscan;

import java.io.Console;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.ArrayList; // import the ArrayList class
import java.sql.Timestamp;  
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import java.io.FileWriter;
import java.util.concurrent.TimeUnit;

/**
 *
 * @author vrosinac
 */
public class Loadscan {
    public static void main(String[] args) throws IOException {
        String jdbc ="hello" ;
        String zoneProfile = "hello" ;
        String zonePassword ="hello" ;
        String start = "hello" ;
        String end = "hello" ;
        String dbType ="Oracle";
        Connection zoneConnection;
        boolean creatingJDBCObjectZone= false;
         Statement stmt;
        ResultSet rs1;
        String sqlQuery="";
        String usage = "usage: java - jar Loadscan.jar [jdbc url]   [Zone profile] [start date-time with format 'YYYY-MM-DD HH24:MI:SS'] [end date-time with format 'YYYY-MM-DD HH24:MI:SS']";
        if (args.length == 4) {
            // System.out.println("we have 2 arguments");
            if (!args[0].isEmpty()) {
                     jdbc = args[0];
            }
            if (!args[1].isEmpty()) {
                     zoneProfile = args[1];
            }
            if (!args[2].isEmpty()) {
                     start = args[2];
            }
          
            if (!args[3].isEmpty()) {
                     end = args[3];
            }
          int debug=1;
            if (debug !=1)
            {
                Console console = System.console();
                char[] password = console.readPassword("Zone password: ");   
                zonePassword = new String(password);
            }
            else
            {
                zonePassword="TIZONE1102";
            }
           
            // System.out.println(zonePassword);



        }
        else
        {
            System.out.println(usage);
            System.exit(0);    
            // TEST DATA 
           /* jdbc="jdbc:db2://mancswgtb0022:50000/FBTI";
            globalProfile = "TIGLOBAL";
            zoneProfile ="TIZONE";
            globalProfile ="TIGLOBAL";
            jobId="9512";
            zonePassword = "T!Z0N3123";
            globalPassword="G10b@!123";
            */
        }    
          
        //need to decide dbtype from connection string
        
        if (jdbc.contains("db2"))
        {
            dbType="DB2";
        }
        
        if (jdbc.contains("oracle"))
        {
            dbType="Oracle";
        }
         
        if (jdbc.contains("sqlserver"))
        {
            dbType="sqlserver";
        }
          
        if (dbType.equals("DB2") ) 
        {
            try 
            {

                ClassLoader cl = ClassLoader.getSystemClassLoader();;
                Class.forName("com.ibm.db2.jcc.DB2Driver", false,cl);
                

            } catch (ClassNotFoundException e) {
       
                    e.toString();
            }
        }
        if (dbType.equals("Oracle") ) 
        {

            try {

                    ClassLoader cl = ClassLoader.getSystemClassLoader();;
                    Class.forName("oracle.jdbc.driver.OracleDriver",false,cl);
                    
            } catch (ClassNotFoundException e) {
                   
                    e.toString();
            }

        }
        
         if (dbType.equals("MsSQLServer")) 
         {
             //not sure what to do there
                   
         }

        
              // Create the connection using the IBM Data Server Driver for JDBC
        if (!zoneProfile.isEmpty() && !zonePassword.isEmpty()
                              && !jdbc.isEmpty()) 
        {
            try 
            {

                        zoneConnection = DriverManager.getConnection(jdbc, zoneProfile,
                                        zonePassword);
                        System.out.println( " **** Got connection with zone");
                        creatingJDBCObjectZone= true;
                        
            
                
                
                /////////////////**//////////
                // Create the Statement

                if (creatingJDBCObjectZone ) 
                {
                    try {
                        
                        
                          /// JobControl ----------------------------------
                        System.out.println( " **** Gatherinng eventstep data");
                            stmt = zoneConnection.createStatement();
                         // todo add: refno_PFIX   + refno serial    -> event
                         /*
                         T1.REFNO_PFIX || ltrim(to_char(T1.REFNO_SERL,'000')) as EVENT_REF
                         */
                            sqlQuery = "SELECT T2.MASTER_REF,T1.REFNO_PFIX || ltrim(to_char(T1.REFNO_SERL,'000')) as EVENT_REF,T7.DESCR, T5.status,T5.startdTIME,T5.LSTMODTIME, T5.LSTMODUSER\n" +
                                    "  FROM BASEEVENT T1  \n" +
                                    " INNER JOIN MASTER T2  \n" +
                                    "  ON T2.KEY97=T1.MASTER_KEY  \n" +
                                    "  INNER JOIN EVENTSTEP T5  \n" +
                                    " ON T5.EVENT_KEY = T1.KEY97  \n" +
                                    " INNER JOIN ORCH_MAP T6  \n" +
                                    "  ON T6.KEY97 = T5.ORCH_MAP  \n" +
                                    "  INNER JOIN ORCH_STEP T7  \n" +
                                    "  ON T7.KEY97 = T6.ORCH_STEP      \n" +
                                     
                                   /* + "" WHERE ( ( T7.DESCR = 'Release' or  T7.DESCR = 'Review')\n" +  */
                                    " WHERE ( (T5.startdtime >= TO_TIMESTAMP('" + start +"','YYYY-MM-DD HH24:MI:SS') )\n" +
                                    " AND (T5.lstmodtime <= TO_TIMESTAMP('" + end +"','YYYY-MM-DD HH24:MI:SS'))\n" +
                                    ")\n";
                            // LSTMODTIME
                            rs1 = stmt.executeQuery(sqlQuery);
                            
                            String result = "master1,evtref1,evtstep1,startTime,endTime,master2,evtref2, evtstep2,startTime,endTime,concurencyHits, concurrentTime(ms)\n";
                            //ResultSetMetaData rsmd=rs1.getMetaData();
                            //int ncols = rsmd.getColumnCount();   
                            String master_ref;
                            master_ref ="";
                            String evtref; 
                            evtref="";
                            String evtstep; 
                            evtstep="";
                            ArrayList<TIEvent> events = new ArrayList<TIEvent>();
                            int nRows=0;
                            while (rs1.next()) 
                            {
                              //store it in our in our own memory
                              //1 master ref
                              // 2 refno-prefix
                              //3 review
                              // 4 status
                              //5 start
                              //6 end                           
                              master_ref =    rs1.getString(1);
                              evtref =  rs1.getString(2);
                              evtstep =    rs1.getString(3);
                              Timestamp start1 =  rs1.getTimestamp(5); 
                              Timestamp end1 =  rs1.getTimestamp(6);
                              TIEvent evt = new TIEvent(master_ref,evtref,evtstep,start1,end1); 
                              events.add(evt);
                              nRows++;
                             }
                            
                            // no we can work from the arraylist
                            /*
                            je prend i					
                        	je prends j				
                                si j.startdtime < i.endtime				
                                    concurencyhits++				
                                    concurrencyendtime = min (j.endtime, i.endtime)				
                                    concurencystartime=max(j.startdtime, i.startdtime)				
                                    concurrencytime = concurencytime + concurencyendtime - concurencystarttime			
                              */
                            int i1, j1, concurencyHits;
                            long concurencyTime;
                            concurencyHits=0;
                            concurencyTime=0;
                            
                            for (i1=0; i1 < nRows; i1++)
                            {
                                for (j1=i1+1; j1< nRows -i1; j1++)
                                {
                                    TIEvent ei1, ej1;
                                    ei1 = events.get(i1);
                                    master_ref = ei1.master_ref;
                                    evtref = ei1.evtref;
                                    ej1 = events.get(j1);
                                    
                                    if (   (ei1.start.compareTo( ej1.end) <0)  &&  (ej1.start.compareTo(ei1.end) <0 )    )
                                    {  // ei1 starts before the end of ej1 and ej1 starts before the end of ei1
                                        //concurency time
                                        Timestamp startOverlap, endOverlap; 
                                        if (!ei1.master_ref.equals(ej1.master_ref)  ) //we only check overlap to other transactions of course
                                        {   
                                            concurencyHits++;
                                            if(ei1.start.compareTo( ej1.start) <0)
                                            {
                                                startOverlap = ej1.start;
                                            }
                                            else 
                                            {
                                                startOverlap = ei1.start;
                                            }
                                            if (ei1.end.compareTo( ej1.end) <0)
                                            {
                                                endOverlap = ei1.end;
                                            }
                                            else 
                                            {
                                                endOverlap = ej1.end;
                                            }

                                            long duration = endOverlap.getTime() - startOverlap.getTime();

                                            //long seconds = TimeUnit.MILLISECONDS.toSeconds(duration);
                                            concurencyTime = concurencyTime +  duration ; // in ms
                                             //write the concurrency result about this master/event
                                            result = result + ei1.master_ref + "," + ei1.evtref + "," +ei1.evtstep + ","  + ei1.start + "," +  ei1.end + "," + ej1.master_ref +"," + ej1.evtref +"," +ej1.evtstep + ","  +ej1.start + "," + ej1.end + "," + concurencyHits + "," + concurencyTime + "\n";
                                            concurencyHits =0;
                                            concurencyTime =0;
                                        }
                                    }
                                }
                            }
                            
                            // then output the resuts to a flat file
                            FileWriter fileWriter = new FileWriter("concurrency.txt");
                            fileWriter.write(result);
                            fileWriter.close();                             
                    
                    } catch (Exception e) {
                            System.out.println(" **** EXCEPTION in Created 1st JDBC Statement object");
                            e.printStackTrace();
                    }

                }

            }
            catch (Exception e) {
                 e.toString();
            }
        }    
        // TODO ... test it on soem of my data
        // TODO ..... maybe do the above on multiple intervals
        
        // ZIP IT ALL UP
       /* System.out.println( " **** Creating the zip archive");
    
        
        List<String> srcFiles = Arrays.asList("JobControl.csv", "StepControl.csv","batchstep.csv","batchmsg.csv","sstuningparamdef.csv","asyncallocation.csv");
        FileOutputStream fos = new FileOutputStream("multiCompressed.zip");
        ZipOutputStream zipOut = new ZipOutputStream(fos);
        for (String srcFile : srcFiles) {
            File fileToZip = new File(srcFile);
            FileInputStream fis = new FileInputStream(fileToZip);
            ZipEntry zipEntry = new ZipEntry(fileToZip.getName());
            zipOut.putNextEntry(zipEntry);

            byte[] bytes = new byte[1024];
            int length;
            while((length = fis.read(bytes)) >= 0) {
                zipOut.write(bytes, 0, length);
            }
            fis.close();
        }
        zipOut.close();
        fos.close();
        
   
          // Delete the files now.
        for (String srcFile : srcFiles) {
            File fileToZip = new File(srcFile);
            fileToZip.delete();
        }
         System.out.println(" **** Success");
     */
    }
  
   
}


class TIEvent
{
    public TIEvent(String m, String e, String s, Timestamp start1, Timestamp end1)
    {
            master_ref=m;
            evtref =e;
            evtstep= s;
            start = start1;
            end = end1;
            
    }        
    public String master_ref;
    public String evtref;
    public String evtstep;
    public Timestamp  start;
    public Timestamp end;
}