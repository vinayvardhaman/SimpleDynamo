package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
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
    private String predecessor = "";
    private String successor = "";
    private String portStr = "";
    private  Object myobject = new Object();
    private boolean condition = false;
    private String queryvalue = "";
    private String Gqueryvalues = "";

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

        if(selection.equals("*"))
        {
            for(int i=0; i< Keys.size(); i++)
            {
                getContext().deleteFile(Keys.get(i));
            }
            Keys.clear();

            //send msg to successor to delete
            String datatosend = "delete";
            datatosend += seperator;
            datatosend += portStr;

            sendMsg(2*Integer.parseInt(successor),datatosend);
        }
        else if(selection.equals("@"))
        {
            for(int i=0; i< Keys.size(); i++)
            {
                getContext().deleteFile(Keys.get(i));
            }
            Keys.clear();
        }
        else
        {
            getContext().deleteFile(selection);
            Keys.remove(selection);
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

        FileOutputStream outputStream;

        try {

            String keyhash = genHash(key);
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
            replicadata += seperator;
            replicadata += Integer.toString(1);

            if(id.equals(successor_id))
            {
                //insert to me
                outputStream = getContext().openFileOutput(key, Context.MODE_PRIVATE);
                outputStream.write(value.getBytes());
                outputStream.close();
                Keys.add(key);
                Log.v(TAG,"insert");
                Log.v(TAG, values.toString());

                //Send replica
                sendMsg(2*Integer.parseInt(successor), replicadata);
            }
            else if(id.compareTo(keyhash) > 0)
            {
                if(id.compareTo(predecessor_id) < 0) {
                    // insert to me
                    outputStream = getContext().openFileOutput(key, Context.MODE_PRIVATE);
                    outputStream.write(value.getBytes());
                    outputStream.close();
                    Keys.add(key);
                    Log.v(TAG,"insert");
                    Log.v(TAG, values.toString());

                    //Send replica
                    sendMsg(2*Integer.parseInt(successor), replicadata);

                }
                else if(predecessor_id.compareTo(keyhash) > 0){
                    // forward to predecessor
                    sendMsg(2 * Integer.parseInt(predecessor), datatosend);
                }
                else if(predecessor_id.compareTo(keyhash) < 0)
                {
                    // insert to me
                    outputStream = getContext().openFileOutput(key, Context.MODE_PRIVATE);
                    outputStream.write(value.getBytes());
                    outputStream.close();
                    Keys.add(key);
                    Log.v(TAG,"insert");
                    Log.v(TAG, values.toString());

                    //Send replica
                    sendMsg(2*Integer.parseInt(successor), replicadata);
                }
            }
            else
            {
                if(predecessor_id.compareTo(id) > 0 && predecessor_id.compareTo(keyhash) < 0)
                {
                    // insert to me
                    outputStream = getContext().openFileOutput(key, Context.MODE_PRIVATE);
                    outputStream.write(value.getBytes());
                    outputStream.close();
                    Keys.add(key);
                    Log.v(TAG,"insert");
                    Log.v(TAG, values.toString());

                    //Send replica
                    sendMsg(2*Integer.parseInt(successor), replicadata);
                }
                else
                {
                    //send to successor
                    sendMsg(2 * Integer.parseInt(successor), datatosend);
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
                successor = "5558";
            }
            else if(portStr.equals("5556")){
                predecessor = "5562";
                successor = "5554";
            }
            else if(portStr.equals("5558")) {
                predecessor = "5554";
                successor = "5560";
            }
            else if(portStr.equals("5560")){
                predecessor = "5558";
                successor = "5562";
            }
            else if(portStr.equals("5562")){
                predecessor = "5560";
                successor = "5556";
            }
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
                    sendMsg(2 * Integer.parseInt(successor), datatosend);
                    condition = false;
                    synchronized (myobject) {
                        while (!condition) {
                            try {
                                myobject.wait();
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                        }
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
            }
            else {
                String value = "";
                int keyindex = search(selection);

                Log.v(TAG,"keyindex");
                Log.v(TAG, Integer.toString(keyindex));

                if(keyindex>=0)
                {
                    inputStream = getContext().openFileInput(selection);
                    while (inputStream.read(buff) != -1) {
                        value += new String(buff);
                    }
                    String[] columnValue = {selection,value.trim()};
                    cursor.addRow(columnValue);
                    inputStream.close();

                    Log.v(TAG,"value");
                    Log.v(TAG, value);
                }
                else
                {
                    //send query to successor and wait
                    String datatosend = "query";
                    datatosend += seperator;
                    datatosend += portStr;
                    datatosend += seperator;
                    datatosend += selection;

                    sendMsg(2*Integer.parseInt(successor),datatosend);
                    Log.v(TAG,"Sent query to");
                    Log.v(TAG, successor);
                    Log.v(TAG,datatosend);

                    synchronized (myobject)
                    {
                        while(!condition)
                        {
                            try {
                                myobject.wait();
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                        }
                        condition = false;
                    }

                    value = queryvalue;
                    String[] columnValue = {selection,value.trim()};
                    cursor.addRow(columnValue);
                    Log.v(TAG,"queryvalue");
                    Log.v(TAG, value);
                }

            }

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        //Log.v(TAG,cursor.getString(0));
        return cursor;
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection,
			String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
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

    private int search(String key)
    {
        int pos =0;
        Boolean found = false;
        Iterator<String> itr = Keys.iterator();
        String mkey = "";

        while(itr.hasNext())
        {
            mkey = itr.next();
            //Log.v(TAG,"mkey");
            //Log.v(TAG,mkey);
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

    }

    private void sendMsg(int port, String Msg)
    {
        try {
            Socket socket = new Socket(InetAddress.getByAddress(new byte[] {10,0,2,2}),port);
            DataOutputStream output = new DataOutputStream(socket.getOutputStream());
            output.writeUTF(Msg);
        } catch (IOException e) {
            e.printStackTrace();
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
                    String datatosend = "";

                    Scanner s = new Scanner(dataRead).useDelimiter(seperator);
                    String type = s.next();
                    Log.v(TAG,type);

                    if(type.equals("insert")) {
                        mUri = buildUri("content","edu.buffalo.cse.cse486586.simpledynamo.provider");
                        mContentValue = new ContentValues();
                        mContentValue.put("key",s.next());
                        mContentValue.put("value",s.next());
                        insert(mUri,mContentValue);
                    }
                    else if(type.equals("replica")) {
                        String  key = s.next();
                        String  value = s.next();
                        int count = s.nextInt();

                        FileOutputStream outputStream = getContext().openFileOutput(key, Context.MODE_PRIVATE);
                        outputStream.write(value.getBytes());
                        outputStream.close();
                        Keys.add(key);
                        Log.v(TAG,"insert-replica");
                        Log.v(TAG,"key,value");
                        Log.v(TAG,key);
                        Log.v(TAG, value);

                        if(count == 1) {
                            datatosend = "replica";
                            datatosend += seperator;
                            datatosend += key;
                            datatosend += seperator;
                            datatosend += value;
                            datatosend += seperator;
                            datatosend += Integer.toString(2);

                            sendMsg(2*Integer.parseInt(successor),datatosend);
                        }

                    }
                    else if(type.equals("query"))
                    {
                        String clientport = s.next();
                        if(clientport.equals(portStr))
                        {
                            queryvalue = s.next();
                            Log.v(TAG,"queryvalueserver");
                            Log.v(TAG,queryvalue);
                            synchronized (myobject)
                            {
                                condition = true;
                                myobject.notify();
                            }
                        }
                        else
                        {
                            String searchkey = s.next();
                            if (search(searchkey) >= 0) {
                                byte[] buff = new byte[100];
                                String value = "";
                                FileInputStream inputStream = getContext().openFileInput(searchkey);
                                while (inputStream.read(buff) != -1) {
                                    value += new String(buff);
                                }
                                datatosend = "query";
                                datatosend += seperator;
                                datatosend += clientport;
                                datatosend += seperator;
                                datatosend += value;

                                sendMsg(2 * Integer.parseInt(clientport), datatosend);
                                Log.v(TAG,"foundvalue");
                                Log.v(TAG,value);
                            } else {
                                //forward query to successor
                                sendMsg(2 * Integer.parseInt(successor), dataRead);
                            }
                        }
                    }
                    else if(type.equals("Gquery"))
                    {
                        Log.v(TAG,"GqueryDataread");
                        Log.v(TAG,dataRead);
                        String clientport = s.next();
                        if(clientport.equals(portStr))
                        {
                            String[] reply = dataRead.split(seperator);
                            Log.v(TAG,"length");
                            Log.v(TAG,Integer.toString(reply.length));

                            if(reply.length == 2)
                                Gqueryvalues = null;
                            else
                                Gqueryvalues = s.next();

                            synchronized (myobject)
                            {
                                condition = true;
                                myobject.notify();
                            }
                        }
                        else
                        {
                            //add my key-values and send to successor
                            FileInputStream inputStream;
                            byte[] buff = new byte[100];
                            datatosend = dataRead;
                            //datatosend += seperator;
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

                            sendMsg(2*Integer.parseInt(successor),datatosend);
                        }
                    }
                    else if(type.equals("delete"))
                    {
                        String clientport = s.next();
                        if(!clientport.equals(portStr))
                        {
                            //delete all my keys and forward to successor
                            mUri = buildUri("content","edu.buffalo.cse.cse486586.simpledynamo.provider");
                            delete(mUri,"*",null);

                            sendMsg(2*Integer.parseInt(successor),dataRead);

                        }
                    }

                } catch (IOException e) {
                    e.printStackTrace();
                }

            }
        }
    }
}
