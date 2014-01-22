package org.finra.datagenerator.impl;

import java.io.IOException;
import java.net.URI;
import java.net.URL;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.finra.datagenerator.interfaces.DataOutput;

/**
 * Implementation that provides output to the HDFS Distributed Filesystem
 * @author Meles
 *
 */
public class HDFSFileOutput implements DataOutput {

	private String hdfsHost;
	private String path;
	
	/**
	 * Constructor that takes in a URI of the HDFS file location to write to
	 * @param uri
	 */
	public HDFSFileOutput(URI uri){
		this.hdfsHost = uri.getScheme() + "://" + uri.getHost() + ":" + uri.getPort();
		this.path = uri.getPath().substring(1);
	}
	
	@Override
	public void outputData(byte[][] data) throws IOException {
		Configuration conf = new Configuration();
        conf.set("fs.defaultFS", this.hdfsHost);
		Path pt=new Path(this.path);
		
        FileSystem fs = FileSystem.get(pt.toUri(), conf);
        
        FSDataOutputStream fos = null;
        try{
	        fos = fs.create(new Path(this.path));
	        for(int i = 0; i < data.length; i++){
				fos.write((new String(data[i]) + "\n").getBytes());
			}
        }catch(Exception e){
        	e.printStackTrace();
        }finally{
        	fos.close();
        }
        
	}

}
