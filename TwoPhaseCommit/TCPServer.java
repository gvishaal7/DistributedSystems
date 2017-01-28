/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
//package tcpapplication;

//import com.sun.xml.internal.ws.wsdl.parser.InaccessibleWSDLException;
import java.io.*;
import java.util.*;
import java.net.*;
import java.text.*;
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
//############################################################
//Comment the below section and remove the comment for lines 00 to 84 to use static server IPs
//############################################################

            input = new BufferedReader(new InputStreamReader(System.in));
            System.out.println("Enter the details of the other servers that are in the network");
            int i =0;
            while(i <4)
            {
                System.out.println("Enter the ip address or dns name of the webserver ("+(++i)+") : ");
                String ipAddress = input.readLine();
                otherServers.add(ipAddress);
            }
//            otherServers.add("172.22.71.30");
//            otherServers.add("172.22.71.31");
//            otherServers.add("172.22.71.32");
//            otherServers.add("172.22.71.33");
//            otherServers.add("172.22.71.34");
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
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("dd-MM-yyyy HH:mm:ss:SSS");
            Date date = new Date();
            try
            {
                //System.out.println(Thread.activeCount());
                inFromClient = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                outToClient = new PrintWriter(socket.getOutputStream(),true);
                while(true)
                {
                    String output = "";
                    String input = inFromClient.readLine();
                    if(input.startsWith("%%"))
                    {
                        updateKeyValueStore(input);
                    }
                    else
                    {
                        StringTokenizer stringTokenizer = new StringTokenizer(input, ",");
                        ArrayList inputParts = new ArrayList();
                        while(stringTokenizer.hasMoreTokens())
                        {
                            inputParts.add(stringTokenizer.nextElement());
                        }
                        output = executeTheOperation(inputParts);
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
                    }
                    writeKeyValuePairToFile();
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
        
        public static void writeKeyValuePairToFile()
        {
            FileWriter fileWriter = null;
            BufferedWriter fileBufferedWriter = null;
            PrintWriter filePrintWriter = null;
            try
            {
                File file = new File("keyValuePair.csv");
                fileWriter = new FileWriter(file);
                fileBufferedWriter = new BufferedWriter(fileWriter);
                filePrintWriter = new PrintWriter(fileBufferedWriter);
                String toFile = "";
                //filePrintWriter.println(toFile);
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
                    int i =0;
                    while(i<4)
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
                        i++;
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
                int i =0;
                while(i<4)
                {
                    String address = (String)otherServers.get(i);
                    socket = new Socket(address, 8080);
                    System.out.println("A healthy connection is established with "+address+" at port 8080");
                    socket.close();
                    i++;
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
