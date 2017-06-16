
import java.io.*;
import java.util.*;
import java.net.*;
import java.text.DecimalFormat;
/**
 *
 * @author Vishaal
 */
public class TCPClient {
    
    public static void main(String[] args) throws Exception {
        BufferedReader input = null;
        try
        {
            input = new BufferedReader(new InputStreamReader(System.in));
            System.out.println("Enter the ip address or the website name");
            String address = input.readLine();
            Socket socket=null;
            try
            {
                socket = new Socket(address,8080);
                System.out.println("Connection with the server has been established");
                System.out.println("Do you want to upload a file or input values individually?(file/ind)");
                String choice = input.readLine();
                options(choice,socket);
            }
            catch(Exception e)
            {
                System.err.println("Connection with the server cannot be established at this moment");
            }
            finally
            {
                try
                {
                    if(socket!=null)
                        socket.close();
                    if(input!=null)
                        input.close();
                }
                catch(Exception e)
                {
                    e.printStackTrace();
                }
            }
        }
        catch(Exception e)
        {
            e.printStackTrace();
        }
    }
    
    public static void options(String choice, Socket socket)
    {
        BufferedReader input = null;
        try
        {
            if(choice.equalsIgnoreCase("ind"))
                sendSingleValuesToServer(socket);
            else if(choice.equalsIgnoreCase("file"))
                sendFileValuesToServer(socket);
            else
            {
                input = new BufferedReader(new InputStreamReader(System.in));
                System.out.println("Enter either ind/file"); 
                choice = input.readLine();
                options(choice, socket);
            }
        }
        catch(Exception e)
        {
            e.printStackTrace();
        }
        finally
        {
            try
            {
                if(input!=null)
                    input.close();
            }
            catch(Exception e)
            {
                e.printStackTrace();
            }
        }
    }
    
    public static void sendFileValuesToServer(Socket socket)
    {
        BufferedReader input = null;
        BufferedReader fileReader = null;
        BufferedReader inFromServer = null;
        PrintWriter outToServer =null;
        try
        {
            input = new BufferedReader(new InputStreamReader(System.in));
            inFromServer = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            outToServer = new PrintWriter(socket.getOutputStream(),true);
            System.out.println("Enter the full file path : ");
            String filePath = input.readLine();
            File file = new File(filePath);
            fileReader = new BufferedReader(new FileReader(file));
            String line = "";
            while((line=fileReader.readLine()) != null)
            {
                //System.out.println("line :: "+line);
                String operation = line.substring(0, line.indexOf(","));
                long startTime = System.nanoTime();
                outToServer.println(line);
                System.out.println(inFromServer.readLine());
                long endTime = System.nanoTime();
                String difference = String.valueOf(endTime - startTime);
                writeTimeFile(difference, operation);
            }
        }
        catch(Exception e)
        {
            e.printStackTrace();
        }
        finally
        {
            try
            {
                if(input!=null)
                    input.close();
                if(fileReader!=null)
                    fileReader.close();
                if(inFromServer!=null)
                    inFromServer.close();
                if(outToServer!=null)
                    outToServer.close();
            }
            catch(Exception e)
            {
                e.printStackTrace();
            }
        }
    }
    
    public static void writeTimeFile(String difference, String operation)
    {
        File file = new File("tcpClientTime.txt");
        FileWriter fileWriter = null;
        try
        {
            fileWriter = new FileWriter(file, true);
            BufferedWriter fileBufferedWriter = new BufferedWriter(fileWriter);
            PrintWriter filePrintWriter = new PrintWriter(fileBufferedWriter);
            try
            {
                filePrintWriter.println(operation.toUpperCase()+":"+difference+" ns");
            }
            catch(Exception e)
            {
                e.printStackTrace();
            }
            finally
            {
                try
                {
                    if(fileBufferedWriter!=null)
                        fileBufferedWriter.close();
                    if(filePrintWriter!=null)
                        filePrintWriter.close();
                }
                catch(Exception e)
                {
                    e.printStackTrace();
                }
            }
            computeAnalysis(file);
        }
        catch(Exception e)
        {
            e.printStackTrace();
        }
        finally
        {
            try
            {
                if(fileWriter!=null)
                    fileWriter.close();
            }
            catch(Exception e)
            {
                e.printStackTrace();
            }
        }
    }
    
    public static void computeAnalysis(File file)
    {
        BufferedReader fileReader = null;
        DecimalFormat numberFormat = new DecimalFormat("#.00");
        try
        {
            fileReader = new BufferedReader(new FileReader(file));
            String line = "";
            Double totalTime = 0.0;
            int lines =0;
            ArrayList timeList = new ArrayList();
            while((line = fileReader.readLine()) != null)
            {
                int startIndex = line.indexOf(":")+1;
                int endIndex = line.indexOf("ns")-1;
                Double time = Double.parseDouble(line.substring(startIndex, endIndex));
                timeList.add(time);
                totalTime+= time;
                lines++;
            }
            
            Double average = (totalTime/lines);
            Double variance = 0.0;
            for(int index=0;index<timeList.size();index++)
            {
                variance += (((Double)timeList.get(index)- average)*((Double)timeList.get(index)- average));
            }
            variance = variance/lines;
            Double standardDeviation = (Double)Math.sqrt(variance);
            
            File newFile = new File("tcpAnalysisReport.txt");
            FileWriter newFileWriter = new FileWriter(newFile);
            BufferedWriter newFileBufferedWriter = new BufferedWriter(newFileWriter);
            PrintWriter newFilePrintWriter = new PrintWriter(newFileBufferedWriter);
            try
            {
                newFilePrintWriter.println("Average : "+numberFormat.format(average));
                newFilePrintWriter.println("Standard Deviation : "+numberFormat.format(standardDeviation));
            }
            catch(Exception e)
            {
                e.printStackTrace();
            }
            finally
            {
                try
                {
                    if(newFilePrintWriter!=null)
                        newFilePrintWriter.close();
                    if(newFileBufferedWriter!=null)
                        newFileBufferedWriter.close();
                    if(newFileWriter!=null)
                        newFileWriter.close();
                }
                catch(Exception e)
                {
                    e.printStackTrace();
                }
            }
        }
        catch(Exception e)
        {
            e.printStackTrace();
        }
        finally
        {
            try
            {
                if(fileReader!=null)
                    fileReader.close();
            }
            catch(Exception e)
            {
                e.printStackTrace();
            }
        }
    }
    
    public static void sendSingleValuesToServer(Socket socket)
    {
        BufferedReader inFromServer = null;
        PrintWriter outToServer =null;
        StringBuilder sb = new StringBuilder();
        BufferedReader input = new BufferedReader(new InputStreamReader(System.in));
        try
        {
                inFromServer = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                outToServer = new PrintWriter(socket.getOutputStream(),true);
                System.out.println("Enter the operation you want to perform(put/get/delete)");
                String operation = input.readLine();
                sb.append(operation+",");
                System.out.println("Enter the key");
                String key = input.readLine();
                sb.append(key);
                if(operation.equalsIgnoreCase("put"))
                {
                    System.out.println("Enter the value");
                    String value = input.readLine();
                    sb.append(","+value);
                }
                long startTime = System.nanoTime();
                outToServer.println(sb.toString());
                System.out.println(inFromServer.readLine());
                long endTime = System.nanoTime();
                String difference = String.valueOf(endTime - startTime);
                writeTimeFile(difference, operation);
            }
            catch(Exception e)
            {
                e.printStackTrace();
            }
    }
}
