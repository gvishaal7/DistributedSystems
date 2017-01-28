/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */



import java.io.*;
import java.util.*;
import java.net.*;
import java.text.*;
import javax.net.ssl.SSLSocket;
import javax.print.attribute.standard.MediaSize;
/**
 *
 * @author Vishaal
 */
public class TCPServer {

    /**
     * @param args the command line arguments
     */
    
    private static Hashtable keyValuePair = new Hashtable();
    private static ArrayList otherServers = new ArrayList();
    private static long seqNo = 1;
    private static String currentStatus = "";
    private static ArrayList otherProcesses = new ArrayList();
    private static String majorOperation = "";
    private static long majorSequenceNumber = 2;
    
    public static void main(String[] args) {
        // TODO code application logic here
        try
        {
            System.out.println("The Server has started running");
            getOtherServers();
            ServerSocket serverSocket = new ServerSocket(8080);
            try
            {
                while(true)
                {
                    new execute(serverSocket.accept()).start();
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
                    serverSocket.close();
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
    
    public static void getOtherServers()
    {
        BufferedReader input= null;
        try
        {
//            input = new BufferedReader(new InputStreamReader(System.in));
//            System.out.println("Enter the details of the other servers that are in the network");
//            int i =0;
//            while(i <4)
//            {
//                System.out.println("Enter the ip address or dns name of the webserver ("+(++i)+") : ");
//                String ipAddress = input.readLine();
//                otherServers.add(ipAddress);
//            }
            otherServers.add("172.22.71.30");
            otherServers.add("172.22.71.31");
            otherServers.add("172.22.71.32");
            otherServers.add("172.22.71.33");
            otherServers.add("172.22.71.34");
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
    
    public static class execute extends Thread
    {
        private Socket socket;
        
        public execute(Socket s)
        {
            this.socket = s;
            System.out.println("The client "+s.getInetAddress()+" is connected through "+s.getPort());
        }
        
        public void run()
        {
            BufferedReader inFromClient = null;
            PrintWriter outToClient=null;
            
            try
            {
                //System.out.println(Thread.activeCount());
                inFromClient = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                outToClient = new PrintWriter(socket.getOutputStream(),true);
                String reqIPAddress = socket.getInetAddress().toString();
                while(true)
                {
                    String output = "";
                    String input = inFromClient.readLine();
                    if(input.startsWith("%%"))
                    {
                        updateKeyValueStore(input);
                    }
                    else if(input.startsWith("&&"))
                    {
                        updateSequenceNumber(input,reqIPAddress);
                    }
                    else if(input.endsWith("%%"))
                    {
                        checkOnProcess(input);
                    }
                    else
                    {
                        ArrayList inputParts = new ArrayList();
                        inputParts = getInput(input);
                        currentStatus = seqNo+","+input+"%%";
                        checkSeqNumber();
                        Boolean status = setSeqNumber();
                        if(status == true)
                        {
                            output = executeTheOperation(inputParts);
                        }
                        else
                        {
                            inputParts = getInput(majorOperation);
                            output = executeTheOperation(inputParts);
                            inputParts = getInput(input);
                            output = executeTheOperation(inputParts);
                        }
                        seqNo = majorSequenceNumber +1;
                        currentStatus = "";
                        writeOnServerLogFile(output, outToClient, socket, inputParts);
                    }
                }
            }
            catch(Exception e)
            {
                //e.printStackTrace();
            }
            finally
            {
                try
                {
                    inFromClient.close();
                    outToClient.close();
                    socket.close();
                    System.out.println("Connection with "+socket.getInetAddress()+ " through "+socket.getPort()+" is closed");
                    
                }
                catch(Exception e)
                {
                    e.printStackTrace();
                }
            }
        }
        
        public static ArrayList getInput(String input)
        {
            ArrayList inputParts = new ArrayList();
            try
            {
                 StringTokenizer stringTokenizer = new StringTokenizer(input, ",");
                 while(stringTokenizer.hasMoreTokens())
                 {
                    inputParts.add(stringTokenizer.nextElement());
                 }
            }
            catch(Exception e)
            {
                e.printStackTrace();
            }
            return inputParts;
        }
        
        public static void writeOnServerLogFile(String output, PrintWriter outToClient,Socket socket,ArrayList inputParts)
        {
            try
            {
                SimpleDateFormat simpleDateFormat = new SimpleDateFormat("dd-MM-yyyy HH:mm:ss:SSS");
                Date date = new Date();
                if(!output.equals(""))
                {
                    outToClient.println(output);
                    File file = new File("tcpServerLog.txt");
                    FileWriter fileWriter = new FileWriter(file,true);
                    BufferedWriter fileBufferedWriter = new BufferedWriter(fileWriter);
                    PrintWriter filePrintWriter = new PrintWriter(fileBufferedWriter);
                    try
                    {
                        //System.out.println(simpleDateFormat.format(date)+" "+socket.getInetAddress()+":"+socket.getPort()+" "+inputParts.toString());
                        filePrintWriter.println(simpleDateFormat.format(date)+" "+socket.getInetAddress()+":"+socket.getPort()+" "+inputParts.toString()+" "+"'"+output+"'");
                    }
                    catch(Exception e)
                    {
                        e.printStackTrace();
                    }
                    finally
                    {
                        if(filePrintWriter!=null)
                            filePrintWriter.close();
                        if(fileBufferedWriter!=null)
                            fileBufferedWriter.close();
                        if(fileWriter!=null)
                            fileWriter.close();
                    }
                }
                writeKeyValuePairToFile();
            }
            catch(Exception e)
            {
                e.printStackTrace();
            }
        }
        
        public static Boolean setSeqNumber()
        {
            Boolean status = false;
            try
            {
                int[] count = new int[otherProcesses.size()];
                for(int i=0;i<otherProcesses.size();i++)
                {
                    String temp = (String)otherProcesses.get(i);
                    for(int j=i+1;j<otherProcesses.size();j++)
                    {
                        if(temp.equals("") && otherProcesses.get(j).equals(temp))
                        {
                            count[i]++;
                        }
                    }
                }
                for(int i=0;i<count.length;i++)
                {
                    if(count[i] >= (count.length/2))
                    {
                        String tempString = (String)otherProcesses.get(i);
                        majorSequenceNumber = Long.parseLong(tempString.substring(0, tempString.indexOf(",")));
                        status = true;
                        break;
                    }
                }
                count = new int[otherProcesses.size()];
                if(status == false)
                {
                    for(int i=0;i<otherProcesses.size();i++)
                    {
                        String temp = (String)otherProcesses.get(i);
                        for(int j=i+1;j<otherProcesses.size();j++)
                        {
                            if(otherProcesses.get(j).equals(temp))
                            {
                                count[i]++;
                            }
                        }
                        if(count[i]>=(count.length/2))
                        {
                            majorOperation = temp.substring(temp.indexOf(","), temp.length()-2);
                            majorSequenceNumber = Long.parseLong(temp.substring(0, temp.indexOf(",")));
                        }
                    }
                }
            }
            catch(Exception e)
            {
                e.printStackTrace();
                status = false;
            }
            return status;
        }
       
        public static void checkOnProcess(String input)
        {
            try
            {
//                String seqNumber = input.substring(0,input.indexOf(","));
//                String operation = input.substring(input.indexOf(",")+1,input.length()-2);
//                ArrayList tempList = new ArrayList();
//                tempList.add(seqNo);
//                tempList.add(operation);
                otherProcesses.add(input);
            }
            catch(Exception e)
            {
                e.printStackTrace();
            }
        }
        
        public static void updateSequenceNumber(String input,String IPAddress)
        {
            try
            {
                long number = Long.parseLong(input.substring(2));
                if(seqNo > number)
                {
                    Socket socket = null;
                    PrintWriter printWriter = null;
                    try
                    {
                            socket = new Socket(IPAddress,8080);
                            printWriter = new PrintWriter(socket.getOutputStream(), true);
                            printWriter.println(currentStatus);
                            socket.close();
                    }
                    catch(Exception e)
                    {
                        e.printStackTrace();
                    }
                    finally
                    {
                        try
                        {
                            if(printWriter!=null)
                                printWriter.close();
                            if(socket!=null)
                                socket.close();
                        }
                        catch(Exception e)
                        {
                            e.printStackTrace();
                        }
                    }
                }
                else
                {
                    seqNo = number;
                }
            }
            catch(Exception e)
            {
                e.printStackTrace();
            }
        }
        
        public static void writeKeyValuePairToFile()
        {
            FileWriter fileWriter = null;
            BufferedWriter fileBufferedWriter = null;
            PrintWriter filePrintWriter = null;
            try
            {
                File file = new File("keyValuePair.csv");
                fileWriter = new FileWriter(file,true);
                fileBufferedWriter = new BufferedWriter(fileWriter);
                filePrintWriter = new PrintWriter(fileBufferedWriter);
                String toFile = "Key,Value";
                filePrintWriter.println(toFile);
                for(Enumeration en = keyValuePair.keys();en.hasMoreElements();)
                {
                    String key = (String)en.nextElement();
                    String value = (String)keyValuePair.get(key);
                    toFile = key+","+value;
                    filePrintWriter.println(toFile);
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
                    if(filePrintWriter!=null)
                        filePrintWriter.close();
                    if(fileBufferedWriter!=null)
                        fileBufferedWriter.close();
                    if(fileWriter!=null)
                        fileWriter.close();
                }
                catch(Exception e)
                {
                    e.printStackTrace();
                }
            }
        }
        
        public static void updateKeyValueStore(String input)
        {
            try
            {
                String operationString = input.substring(2, input.length());
                String[] inputParts = operationString.split(":");
                if(inputParts[0].equalsIgnoreCase("put"))
                {
                    keyValuePair.put(inputParts[1],inputParts[2]);
                }
                else if(inputParts[0].equalsIgnoreCase("delete"))
                {
                    keyValuePair.remove(inputParts[1]);
                }
            }
            catch(Exception e)
            {
                e.printStackTrace();
            }
        }
        
        public static Boolean updateOnOtherServers(ArrayList inputParts)
        {
            Boolean val = false;
            Socket socket = null;
            PrintWriter outToOtherServers = null;
            try
            {
                Boolean status = checkServerHealth();
                if(status == true)
                {
                    updateLocalStorage(inputParts);
                    for(int i=0;i<otherServers.size();i++)
                    {
                        String address = (String)otherServers.get(i);
                        socket = new Socket(address,8080);
                        outToOtherServers = new PrintWriter(socket.getOutputStream(), true);
                        String infoToOtherServers = "%%";
                        for(int index=0;index<inputParts.size();index++)
                        {
                            infoToOtherServers += (String)inputParts.get(index)+":";
                        }
                        outToOtherServers.println(infoToOtherServers);
                        socket.close();
                    }
                    val = true;
                }
                else
                {
                    val = false;
                }
            }
            catch(Exception e)
            {
                e.printStackTrace();
                val = false;
            }
            finally
            {
                try
                {
                    if(outToOtherServers!=null)
                        outToOtherServers.close();
                    if(socket!=null)
                        socket.close();
                }
                catch(Exception e)
                {
                    e.printStackTrace();
                }
            }
            return val;
        }
        
        public static void checkSeqNumber()
        {
            Socket socket = null;
            PrintWriter printWriter = null;
            try
            {
                String toOtherServers = "&&"+String.valueOf(seqNo);
                for(int i=0;i<otherServers.size();i++)
                {
                    String address = (String)otherServers.get(i);
                    socket = new Socket(address,8080);
                    printWriter = new PrintWriter(socket.getOutputStream(), true);
                    printWriter.println(toOtherServers);
                    socket.close();
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
                    if(printWriter!=null)
                        printWriter.close();
                    if(socket!=null)
                        socket.close();
                }
                catch(Exception e)
                {
                    e.printStackTrace();
                }
            }
        }
        
        public static void updateLocalStorage(ArrayList inputParts)
        {
            try
            {
                if(((String)inputParts.get(0)).equalsIgnoreCase("put"))
                { 
                    keyValuePair.put((String)inputParts.get(1), (String)inputParts.get(2)); 
                }
                else 
                {
                    keyValuePair.remove((String)inputParts.get(1));
                }
                
            }
            catch(Exception e)
            {
                e.printStackTrace();
            }
        }
        public static Boolean checkServerHealth()
        {
            Boolean status = false;
            Socket socket = null;
            try
            {
                for(int i =0;i<otherServers.size();i++)
                {
                    String address = (String)otherServers.get(i);
                    socket = new Socket(address, 8080);
                    System.out.println("A healthy connection is established with "+address+" at port 8080");
                    socket.close();
                }
                status = true;
            }
            catch(Exception e)
            {
                e.printStackTrace();
                status = false;
            }
            finally
            {
                try
                {
                    if(socket!=null)
                        socket.close();
                }
                catch(Exception e)
                {
                    e.printStackTrace();
                }
            }
            return status;
        }
        
        public static String executeTheOperation(ArrayList inputParts)
        {
            String output = "";
            try
            {
                if(((String)inputParts.get(0)).equalsIgnoreCase("put"))
                { 
 //                   keyValuePair.put((String)inputParts.get(1), (String)inputParts.get(2)); 
                    Boolean val = updateOnOtherServers(inputParts);
                    if(val == true)
                        output = "The new key-value pair has been added";
                    else
                        output = "Failed to add the key-value pair";
                }
                else if(((String)inputParts.get(0)).equalsIgnoreCase("get"))
                {
                    if(keyValuePair.isEmpty())
                    output = "The operation cannot be preformed now!";
                    else
                    {
                        if(keyValuePair.containsKey((String)inputParts.get(1)))
                            output = (String)keyValuePair.get((String)inputParts.get(1));
                        else
                            output = "The key does not exist";
                    }
                }
                else if(((String)inputParts.get(0)).equalsIgnoreCase("delete"))
                {
                    if(keyValuePair.isEmpty())
                        output = "The operation cannot be preformed now!";
                    else
                    {
                        if(keyValuePair.containsKey((String)inputParts.get(1)))
                        {
                            //keyValuePair.remove((String)inputParts.get(1));
                            Boolean val = updateOnOtherServers(inputParts);
                            if(val == true)
                                output = "The key-value pair has been removed";
                            else
                                output = "The key-value pair was not removed";
                        }
                        else
                            output = "The key does not exist";
                    }
                }
                else
                {
                    output = "The operation does not exist";
                }
            }
            catch(Exception e)
            {
                e.printStackTrace();
            }
            return output;
        }
    }
    
}
