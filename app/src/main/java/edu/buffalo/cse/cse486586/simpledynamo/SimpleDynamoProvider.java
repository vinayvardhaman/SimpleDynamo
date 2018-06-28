package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.lang.reflect.Type;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.Iterator;
import java.util.List;
import java.util.Scanner;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDynamoProvider extends ContentProvider {

    static final String TAG = SimpleDynamoProvider.class.getSimpleName();
    static final int SERVER_PORT = 10000;
    private  Uri mUri;
    private ContentValues mContentValue;
    private String id = "";
    private String seperator = "/simpledynamo/";
    List<String> Keys = new ArrayList<String>();
    List<String> MyKeys = new ArrayList<String>();
    private String predecessor = "";
    private String predecessor2 = "";
    private String successor = "";
    private String portStr = "";
    private static Object myobject = new Object();
    private static Object deletelock = new Object();
    private static Object insertlock = new Object();
    private boolean condition = false;
    private static  String querykey = "";
    private static  String queryvalue = "";
    private String Gqueryvalues = "";
    private static String failedprocess = "";

	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub
        if(selectionArgs!=null)
        {
            Log.v(TAG,"selectionArgs");
            for(int i=0; i<selectionArgs.length; i++)
            {
                Log.v(TAG,selectionArgs[0]);
            }
        }

        Log.v(TAG, "delete");
        Log.v(TAG,selection);

        synchronized (deletelock) {
            if (selection.equals("*")) {
                for (int i = 0; i < Keys.size(); i++) {
                    getContext().deleteFile(Keys.get(i));
                }
                Keys.clear();
                MyKeys.clear();

                //send msg to successor to delete
                String datatosend = "delete";
                datatosend += seperator;
                datatosend += portStr;

                //sendAllNodes("delete"+seperator+portStr);

                String reply = sendMsg(2 * Integer.parseInt(successor), datatosend);
                if (reply.equals("fail")) {
                    String successor2 = getSuccessor(successor);
                    sendMsg(2 * Integer.parseInt(successor2), datatosend);
                }
            } else if (selection.equals("@")) {
                for (int i = 0; i < Keys.size(); i++) {
                    getContext().deleteFile(Keys.get(i));
                }
                Keys.clear();
                MyKeys.clear();
            } else {

                String coOrdinator = getCoordinator(selection);
                if (coOrdinator.equals(portStr)) {
                    // delete and send to successors to delete
                    getContext().deleteFile(selection);
                    Keys.remove(selection);
                    MyKeys.remove(selection);

                    sendMsg(2 * Integer.parseInt(successor), "deletekey" + seperator + "replica" + seperator + selection);
                    String successor2 = getSuccessor(successor);
                    sendMsg(2 * Integer.parseInt(successor2), "deletekey" + seperator + "replica" + seperator + selection);
                } else {
                    //send to coordinator and its successors
                    sendMsg(2 * Integer.parseInt(coOrdinator), "deletekey" + seperator + "key" + seperator + selection);
                    String successor1 = getSuccessor(coOrdinator);
                    String successor2 = getSuccessor(successor1);
                    sendMsg(2 * Integer.parseInt(successor1), "deletekey" + seperator + "replica" + seperator + selection);
                    sendMsg(2 * Integer.parseInt(successor2), "deletekey" + seperator + "replica" + seperator + selection);
                }


            }
        }

        return 0;
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {
		// TODO Auto-generated method stub
        String key,value;

        key = values.get("key").toString();
        value = values.get("value").toString();
        Log.v(TAG,"insert-call");
        Log.v(TAG,key);

        FileOutputStream outputStream;

        try {

            String keyhash = genHash(key);
            String coOrdinator = getCoordinator(key);
            String successor_id = genHash(successor);
            String predecessor_id = genHash(predecessor);
            String datatosend = "insert";
            datatosend += seperator;
            datatosend += key;
            datatosend += seperator;
            datatosend += value;

            String replicadata = "replica";
            replicadata += seperator;
            replicadata += key;
            replicadata += seperator;
            replicadata += value;

            synchronized (insertlock) {
                if (coOrdinator.equals(portStr)) {
                    // insert to me
                    outputStream = getContext().openFileOutput(key, Context.MODE_PRIVATE);
                    outputStream.write(value.getBytes());
                    outputStream.close();
                    Keys.add(key);
                    MyKeys.add(key);
                    Log.v(TAG, "insert");
                    Log.v(TAG, values.toString());

                    //Send replica
                    sendMsg(2 * Integer.parseInt(successor), replicadata);

                    String successor2 = getSuccessor(successor);
                    sendMsg(2 * Integer.parseInt(successor2), replicadata);
                } else {
                    // send key-value pair to Coordinator and its successors for replicas
                    String reply = sendMsg(2 * Integer.parseInt(coOrdinator), datatosend);
                    Log.v(TAG, "insert sent to coordinator");
                    Log.v(TAG, reply);
                    if (reply.equals("fail")) {
                        String replica1 = getSuccessor(coOrdinator);
                        String replica2 = getSuccessor(replica1);
                        sendMsg(2 * Integer.parseInt(replica1), replicadata);
                        sendMsg(2 * Integer.parseInt(replica2), replicadata);
                    }
                }

            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        return null;
    }

	@Override
	public boolean onCreate() {
		// TODO Auto-generated method stub
		try{
			ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
			serverSocket.setReuseAddress(true);
			new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
		}
		catch (IOException e)
		{
			Log.e(TAG, "Can't create a ServerSocket");
			return false;
		}

        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);

        try {
            id = genHash(portStr);
            Log.v(TAG,"ID");
            Log.v(TAG,id);

            if(portStr.equals("5554")){
                predecessor = "5556";
                predecessor2 = "5562";
                successor = "5558";
            }
            else if(portStr.equals("5556")){
                predecessor = "5562";
                predecessor2 = "5560";
                successor = "5554";
            }
            else if(portStr.equals("5558")) {
                predecessor = "5554";
                predecessor2 = "5556";
                successor = "5560";
            }
            else if(portStr.equals("5560")){
                predecessor = "5558";
                predecessor2 = "5554";
                successor = "5562";
            }
            else if(portStr.equals("5562")){
                predecessor = "5560";
                predecessor2 = "5558";
                successor = "5556";
            }

            String[] files =  getContext().fileList();
            for(int i=0; i<files.length; i++)
            {
                getContext().deleteFile(files[i]);
            }

            String msg = "startup";
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg);

/*
            getKeyValues(2*Integer.parseInt(predecessor),"keys" + seperator + portStr);
            getKeyValues(2*Integer.parseInt(predecessor2), "keys" + seperator + portStr);
            getKeyValues(2*Integer.parseInt(successor), "missedkeys" + seperator + portStr);
*/
  //          sendAllNodes("recovered");

        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        return false;
	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
			String[] selectionArgs, String sortOrder) {
		// TODO Auto-generated method stub
        FileInputStream inputStream;
        byte[] buff = new byte[100];
        char c;

        String[] columnNames = {"key","value"};
        //String[] columnValue = {selection,value.trim()};
        MatrixCursor cursor = new MatrixCursor(columnNames);

        Log.v(TAG,"Query");
        Log.v(TAG, selection);


        try {
            if(selection.equals("*"))
            {
                for(int i=0; i< Keys.size(); i++)
                {
                    String value = "";
                    inputStream = getContext().openFileInput(Keys.get(i));
                    while (inputStream.read(buff) != -1) {
                        value += new String(buff);
                    }
                    String[] columnValue = {Keys.get(i), value.trim()};
                    cursor.addRow(columnValue);
                    inputStream.close();
                }

                String successor_id = genHash(successor);

                if(!id.equals(successor_id))
                {
                    //send Gquery to successor and wait
                    String datatosend = "Gquery";
                    datatosend += seperator;
                    datatosend += portStr;
                    datatosend += seperator;


                    String reply = sendMsg(2 * Integer.parseInt(successor), datatosend);

                    if (reply.equals("fail")) {
                        String successor2 = getSuccessor(successor);
                        sendMsg(2 * Integer.parseInt(successor2), datatosend);
                    }

                    synchronized (myobject) {
                        while (!condition) {
                            try {
                                myobject.wait();
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                        }
                        condition = false;
                    }

                    if(Gqueryvalues!=null)
                    {
                        String[] kvpairs = Gqueryvalues.split("##");

                        for (int i = 0; i < kvpairs.length; i++) {
                            String[] kvpair = kvpairs[i].split("--");
                            cursor.addRow(kvpair);
                        }
                    }
                }

            }
            else if(selection.equals("@"))
            {
                for(int i=0; i< Keys.size(); i++)
                {
                    String value = "";
                    inputStream = getContext().openFileInput(Keys.get(i));
                    while(inputStream.read(buff) != -1)
                    {
                        value += new String(buff);
                    }
                    String[] columnValue = {Keys.get(i),value.trim()};
                    cursor.addRow(columnValue);

                    inputStream.close();
                }
            } else {
                /*
                synchronized (myobject) {
                    querykey = selection;
                    condition = false;
                    String value = "";
                    String coOrdinator = getCoordinator(selection);
                    String coSuccessor1 = getSuccessor(coOrdinator);
                    String coSuccessor2 = getSuccessor(coSuccessor1);
                    Log.v(TAG, "coOrdinator");
                    Log.v(TAG, coOrdinator);

                    if (false) {
                        //Log.v(TAG,"found");
                        inputStream = getContext().openFileInput(selection);
                        while (inputStream.read(buff) != -1) {
                            value += new String(buff);
                        }
                        value = value.trim();
                        String[] columnValue = {selection, value};
                        cursor.addRow(columnValue);
                        inputStream.close();

                        Log.v(TAG, "value");
                        Log.v(TAG, value);
                    } else {
                        //send query to coSuccessor2, coSuccessor1 and wait
                        String datatosend = "query";
                        datatosend += seperator;
                        datatosend += portStr;
                        datatosend += seperator;
                        datatosend += selection;

                        Log.v(TAG, "Sent query to coOrdinator and coSuccessor1");
                        Log.v(TAG, coOrdinator);

                        sendMsg(2 * Integer.parseInt(coOrdinator),datatosend,false);
                        sendMsg(2*Integer.parseInt(coSuccessor1),datatosend,false);
                        Log.v(TAG, "datatosend");
                        Log.v(TAG, datatosend);
                        Log.v(TAG,"condition");
                        Log.v(TAG,condition+"");
                        while (!condition) {
                            try {
                                myobject.wait();
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                        }
                        Log.d(TAG,"Out of while lopp");
                        condition = false;
*/
                        /*if (value.equals("fail")) {
                            //String Coordinator2 = getSuccessor(coSuccessor2);
                            Log.v(TAG, "coOrdinator failed and query sent to coSuccessor1");
                            Log.v(TAG, coSuccessor1);
                            value = sendMsg(2 * Integer.parseInt(coSuccessor1), datatosend);

                            if(value.equals("fail")){
                                Log.v(TAG, "sent query to coSuccessor2");
                                Log.v(TAG, coSuccessor2);
                                value = sendMsg(2 * Integer.parseInt(coSuccessor2), datatosend);
                            }

                        }*/

                        querykey(selection);
                        String[] columnValue = {selection, queryvalue.trim()};
                        cursor.addRow(columnValue);
                        Log.v(TAG, "queryvalue");
                        Log.v(TAG, queryvalue);

         //           }
              //  }

            }

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        return cursor;
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection,
			String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}

	private synchronized String querykey(String key)
    {
        querykey = key;
        condition = false;
        String value = "";
        String coOrdinator = getCoordinator(querykey);
        String coSuccessor1 = getSuccessor(coOrdinator);
        String coSuccessor2 = getSuccessor(coSuccessor1);
        Log.v(TAG, "coOrdinator");
        Log.v(TAG, coOrdinator);

        //send query to coSuccessor2, coSuccessor1 and wait
        String datatosend = "query";
        datatosend += seperator;
        datatosend += portStr;
        datatosend += seperator;
        datatosend += querykey;

        Log.v(TAG, "Sent query to coOrdinator and coSuccessor1");
        Log.v(TAG, coOrdinator);

        sendMsg(2 * Integer.parseInt(coOrdinator),datatosend,false);
        sendMsg(2*Integer.parseInt(coSuccessor1),datatosend,false);
        Log.v(TAG, "datatosend");
        Log.v(TAG, datatosend);
        Log.v(TAG,"condition");
        Log.v(TAG,condition+"");

        while (!condition) {
            /*try {
                myobject.wait();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }*/
        }
        Log.d(TAG,"Out of while lopp");
        condition = false;

        return queryvalue;

    }

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

   /* private int search(String key)
    {
        int pos =0;
        Boolean found = false;
        Iterator<String> itr = Keys.iterator();
        String mkey = "";

        if(Keys.contains(key))


        while(itr.hasNext())
        {
            mkey = itr.next();
            if(key.equals(mkey))
            {
                found = true;
                break;
            }
            pos++;
        }

        Log.v(TAG,"found");
        Log.v(TAG,Boolean.toString(found));

        if(found == true)
            return pos;
        else
            return -1;

    }*/

    private String getSuccessor(String node)
    {
        if(node.equals("5554"))
        {
            return "5558";
        }
        else if(node.equals("5558"))
        {
            return "5560";
        }
        else if(node.equals("5560"))
        {
            return "5562";
        }
        else if(node.equals("5562"))
        {
            return "5556";
        }
        else
        {
            return "5554";
        }

    }

    private String getCoordinator(String key)
    {
        try
        {
            String keyhash = genHash(key);
            String id0 = genHash("5554");
            String id1 = genHash("5556");
            String id2 = genHash("5558");
            String id3 = genHash("5560");
            String id4 = genHash("5562");

            System.out.println(id4.compareTo(id1));
            System.out.println(id1.compareTo(id0));
            System.out.println(id0.compareTo(id2));
            System.out.println(id2.compareTo(id3));
            System.out.println(id3.compareTo(id4));

            if(keyhash.compareTo(id4) >= 0 && keyhash.compareTo(id1) < 0)
            {
                return "5556";
            }
            else if(keyhash.compareTo(id1) >= 0 && keyhash.compareTo(id0) < 0)
            {
                return "5554";
            }
            else if(keyhash.compareTo(id0) >= 0 && keyhash.compareTo(id2) < 0)
            {
                return "5558";
            }
            else if(keyhash.compareTo(id2) >= 0 && keyhash.compareTo(id3) < 0)
            {
                return "5560";
            }
            else
            {
                return "5562";
            }
        }
        catch(NoSuchAlgorithmException e)
        {
            e.printStackTrace();
        }

        return "";

    }
    private String sendMsg(int port, String Msg)
    {
        return sendMsg(port,Msg,true);
    }

    private String sendMsg(int port, String Msg, boolean read)
    {
        String reply = "fail";
        try {
            Socket socket = new Socket(InetAddress.getByAddress(new byte[] {10,0,2,2}),port);
            socket.setSoTimeout(500);
            DataOutputStream output = new DataOutputStream(socket.getOutputStream());
            output.writeUTF(Msg);

            if(read == true) {
                DataInputStream input = new DataInputStream(socket.getInputStream());
                reply = input.readUTF();

                Log.v(TAG, "reply");
                Log.v(TAG, reply);
            }

            /*OutputStream outputStream = socket.getOutputStream();
            BufferedWriter out = new BufferedWriter(new OutputStreamWriter(outputStream));
            out.write(Msg+"\n");

            InputStream inputStream = socket.getInputStream();
            BufferedReader in = new BufferedReader(new InputStreamReader(inputStream));
            reply = in.readLine();*/



        } catch (IOException e) {
            e.printStackTrace();
            failedprocess = Integer.toString(port/2);
            Log.v("process failed ID:",failedprocess);
        }

        return reply;
    }

    private void getKeyValues(int port, String Msg)
    {
        try {
            Socket socket = new Socket(InetAddress.getByAddress(new byte[] {10,0,2,2}),port);
            socket.setSoTimeout(500);
            DataOutputStream output = new DataOutputStream(socket.getOutputStream());
            output.writeUTF(Msg);
            /*OutputStream outputStream = socket.getOutputStream();
            BufferedWriter out = new BufferedWriter(new OutputStreamWriter(outputStream));
            out.write(Msg+"\n");*/
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    private void sendAllNodes(String Msg)
    {
        for(int i=0; i<5; i++)
        {
            try {
                Socket socket = new Socket(InetAddress.getByAddress(new byte[] {10,0,2,2}),11108+4*i);
                socket.setSoTimeout(500);
                DataOutputStream output = new DataOutputStream(socket.getOutputStream());
                output.writeUTF(Msg);
                /*OutputStream outputStream = socket.getOutputStream();
                BufferedWriter out = new BufferedWriter(new OutputStreamWriter(outputStream));
                out.write(Msg+"\n");*/
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }

    private class ClientTask extends AsyncTask<String, Void, Void>
    {
        @Override
        protected Void doInBackground(String... msgs)
        {
          if(msgs[0].equals("startup")){
              getKeyValues(2*Integer.parseInt(predecessor),"keys" + seperator + portStr);
              getKeyValues(2*Integer.parseInt(predecessor2), "keys" + seperator + portStr);
              getKeyValues(2*Integer.parseInt(successor), "missedkeys" + seperator + portStr);

              sendAllNodes("recovered");
              /*Log.v(TAG,"Sending startup");
              Socket socket = null;
              try {
                  socket = new Socket(InetAddress.getByAddress(new byte[] {10,0,2,2}),2*Integer.parseInt(portStr));
                  socket.setSoTimeout(1000);
                  DataOutputStream output = new DataOutputStream(socket.getOutputStream());
                  output.writeUTF(Msg);
                  *//*BufferedWriter out = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
                  out.write("recovered");*//*
              } catch (UnknownHostException e) {
                  e.printStackTrace();
              } catch (SocketException e) {
                  e.printStackTrace();
              } catch (IOException e) {
                  e.printStackTrace();
              }*/

              Log.v(TAG,"Sent startup");

          }
          return null;
        }
    }

    private class ServerTask extends AsyncTask<ServerSocket, String, Boolean>
    {
        @Override
        protected Boolean doInBackground(ServerSocket... sockets){

            ServerSocket serverSocket = sockets[0];

            while(true) {
                Socket newsocket = null;
                try {
                    newsocket = serverSocket.accept();
                    DataInputStream input = new DataInputStream(newsocket.getInputStream());
                    String dataRead = input.readUTF();
                    /*InputStream input = newsocket.getInputStream();
                    BufferedReader in = new BufferedReader(new InputStreamReader(input));
                    String dataRead  = in.readLine();*/
                    Log.v(TAG,"dataRead");
                    Log.v(TAG,dataRead);

                    String datatosend = "";
                    DataOutputStream dataoutputStream = new DataOutputStream(newsocket.getOutputStream());
                    /*OutputStream output = newsocket.getOutputStream();
                    BufferedWriter out = new BufferedWriter(new OutputStreamWriter(output));*/
                    String receivedmsg = "pass";

                    //Scanner s = new Scanner(dataRead).useDelimiter(seperator);
                    String[] dsv = dataRead.split(seperator);
                    String type = dsv[0];
                    Log.v(TAG,type);

                    if(type.equals("insert")) {
                        String  key = dsv[1];
                        String  value = dsv[2];

                        FileOutputStream outputStream = getContext().openFileOutput(key, Context.MODE_PRIVATE);
                        outputStream.write(value.getBytes());
                        outputStream.close();

                        Keys.add(key);
                        MyKeys.add(key);
                        Log.v(TAG,"insert");
                        Log.v(TAG,"key,value");
                        Log.v(TAG,key);
                        Log.v(TAG, value);

                        //send replicas
                        datatosend += "replica";
                        datatosend += seperator;
                        datatosend += key;
                        datatosend += seperator;
                        datatosend += value;

                        sendMsg(2*Integer.parseInt(successor), datatosend);
                        String successor2 = getSuccessor(successor);
                        sendMsg(2*Integer.parseInt(successor2), datatosend);

                        dataoutputStream.writeUTF(receivedmsg);
                        //out.write(receivedmsg);

                    }
                    else if(type.equals("replica")) {
                        String  key = dsv[1];
                        String  value = dsv[2];

                        FileOutputStream outputStream = getContext().openFileOutput(key, Context.MODE_PRIVATE);
                        outputStream.write(value.getBytes());
                        outputStream.close();
                        Keys.add(key);
                        Log.v(TAG,"insert-replica");
                        Log.v(TAG,"key,value");
                        Log.v(TAG,key);
                        Log.v(TAG, value);

                        dataoutputStream.writeUTF(receivedmsg);
                        //out.write(receivedmsg);

                    } else if (type.equals("query")) {
                        String clientport = dsv[1];
                        String searchkey = dsv[2];


                        if (Keys.contains(searchkey)) {

                            Log.v(TAG,"found");
                            byte[] buff = new byte[100];
                            String value = "";
                            FileInputStream inputStream = getContext().openFileInput(searchkey);
                            while (inputStream.read(buff) != -1) {
                                value += new String(buff);
                            }
                            value = value.trim();
                            datatosend = "queryreply";
                            datatosend += seperator;
                            datatosend += searchkey;
                            datatosend += seperator;
                            datatosend += value.trim();

                            Log.v(TAG, "foundvalue");
                            Log.v(TAG, value);
                            sendMsg(2*Integer.parseInt(clientport),datatosend,false);
                            //dataoutputStream.writeUTF(value);
                            //out.write(value+"\n");
                        }
                    }
                    else if(type.equals("queryreply"))
                    {
                        if(dsv[1].equals(querykey) && !dsv[2].equals(queryvalue)) {
                            queryvalue = dsv[2];
                            Log.v(TAG, "querykey, queryvalue");
                            Log.v(TAG, querykey);
                            Log.v(TAG, queryvalue);

                            condition = true;
                            /*synchronized (myobject)
                            {
                                condition = true;
                                myobject.notify();
                            }*/
                        }


                    }
                    else if(type.equals("Gquery"))
                    {
                        Log.v(TAG,"GqueryDataread");
                        Log.v(TAG,dataRead);
                        String clientport = dsv[1];
                        if(clientport.equals(portStr))
                        {
                            String[] reply = dataRead.split(seperator);
                            Log.v(TAG,"length");
                            Log.v(TAG,Integer.toString(reply.length));

                            if(reply.length == 2)
                                Gqueryvalues = null;
                            else
                                Gqueryvalues = dsv[2];

                            synchronized (myobject)
                            {
                                condition = true;
                                myobject.notify();
                            }

                            dataoutputStream.writeUTF(receivedmsg);
                            //out.write(receivedmsg);
                        }
                        else
                        {
                            //add my key-values and send to successor
                            FileInputStream inputStream;
                            byte[] buff = new byte[100];
                            datatosend = dataRead;

                            for(int i=0; i<Keys.size(); i++)
                            {
                                String value = "";
                                inputStream = getContext().openFileInput(Keys.get(i));
                                while(inputStream.read(buff) != -1)
                                {
                                    value += new String(buff);
                                }

                                datatosend += Keys.get(i);
                                datatosend += "--";
                                datatosend += value.trim();
                                datatosend += "##";
                                inputStream.close();
                            }

                            String reply = sendMsg(2*Integer.parseInt(successor),datatosend);

                            if (reply.equals("fail")) {
                                String successor2 = getSuccessor(successor);
                                sendMsg(2 * Integer.parseInt(successor2), datatosend);
                            }

                            dataoutputStream.writeUTF(receivedmsg);
                            //out.write(receivedmsg);
                        }
                    }
                    else if(type.equals("delete"))
                    {
                        String clientport = dsv[1];
                        if(!clientport.equals(portStr))
                        {
                            //delete all my keys and forward to successor
                            mUri = buildUri("content","edu.buffalo.cse.cse486586.simpledynamo.provider");
                            delete(mUri,"*",null);

                            String reply = sendMsg(2*Integer.parseInt(successor),dataRead);

                            if (reply.equals("fail")) {
                                String successor2 = getSuccessor(successor);
                                sendMsg(2 * Integer.parseInt(successor2), dataRead);
                            }

                        }
                        dataoutputStream.writeUTF(receivedmsg);
                        //out.write(receivedmsg);
                    }
                    else if(type.equals("keys"))
                    {
                        String client = dsv[1];
                        //add my key-values and send to client
                        FileInputStream inputStream;
                        byte[] buff = new byte[100];
                        datatosend = "keysreply";
                        datatosend += seperator;
                        datatosend += "replicas";
                        datatosend += seperator;

                        for(int i=0; i<MyKeys.size(); i++)
                        {
                            String value = "";
                            inputStream = getContext().openFileInput(MyKeys.get(i));
                            while(inputStream.read(buff) != -1)
                            {
                                value += new String(buff);
                            }

                            datatosend += MyKeys.get(i);
                            datatosend += "--";
                            datatosend += value.trim();
                            datatosend += "##";
                            inputStream.close();
                        }

                        sendMsg(2*Integer.parseInt(client),datatosend);
                    }
                    else if(type.equals("missedkeys"))
                    {
                        String client = dsv[1];
                        //add key values missed by client and send to client
                        FileInputStream inputStream;
                        byte[] buff = new byte[100];
                        datatosend = "keysreply";
                        datatosend += seperator;
                        datatosend += "missedkeys";
                        datatosend += seperator;

                        for(int i=0; i<Keys.size(); i++)
                        {
                            if(client.equals(getCoordinator(Keys.get(i)))) {
                                String value = "";
                                inputStream = getContext().openFileInput(Keys.get(i));
                                while (inputStream.read(buff) != -1) {
                                    value += new String(buff);
                                }

                                datatosend += Keys.get(i);
                                datatosend += "--";
                                datatosend += value.trim();
                                datatosend += "##";
                                inputStream.close();
                            }
                        }

                        sendMsg(2*Integer.parseInt(client),datatosend);
                    }
                    else if(type.equals("keysreply"))
                    {
                        String keyvalueType = dsv[1];

                        if(dsv.length>2) {
                            String keyvalues = dsv[2];
                            Log.v(TAG, "keyvalueType, keyvalues");
                            Log.v(TAG, keyvalueType);
                            Log.v(TAG, keyvalues);
                            String[] kvpairs = keyvalues.split("##");

                            for (int i = 0; i < kvpairs.length; i++) {
                                String[] kvpair = kvpairs[i].split("--");

                                FileOutputStream outputStream = getContext().openFileOutput(kvpair[0], Context.MODE_PRIVATE);
                                outputStream.write(kvpair[1].getBytes());
                                outputStream.close();

                                Keys.add(kvpair[0]);

                                if (keyvalueType.equals("missedkeys"))
                                    MyKeys.add(kvpair[0]);

                            }
                        }

                        dataoutputStream.writeUTF(receivedmsg);
                       // out.write(receivedmsg);
                    }
                    else if(type.equals("deletekey"))
                    {
                        String keytype = dsv[1];
                        String key = dsv[2];

                        getContext().deleteFile(key);
                        Keys.remove(key);

                        if(keytype.equals("key"))
                            MyKeys.remove(key);

                        Log.v(TAG,"keytype,key");
                        Log.v(TAG, keytype);
                        Log.v(TAG, key);

                        dataoutputStream.writeUTF(receivedmsg);

                    }
                    else if(type.equals("recovered"))
                    {
                        failedprocess = "";
                    }


                    dataoutputStream.close();
                    //out.close();
                    newsocket.close();

                } catch (IOException e) {
                    e.printStackTrace();
                }

            }
        }
    }
}
