/*
 * (C) Copyright 2013 DataGenerator Contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package org.finra.datagenerator.test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.finra.datagenerator.factory.DataOutputFactory;
import org.finra.datagenerator.interfaces.DataOutput;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for the DataOutput interface implementations
 * 
 * @author MeleS
 *
 */
public class DataOutputTest {
	
	private String[] expectedOut = new String[] { "Test 1", "Test 2"};
	private byte[][] testArr = new byte[][] {expectedOut[0].getBytes(), expectedOut[1].getBytes()};
	private final String testFileStr = System.getProperty("user.dir").replaceAll("\\\\", "/") + "/Test.txt";
	private final String factoryTestFileStr = "file://" + testFileStr;
	private final File testFile = new File(factoryTestFileStr);

	/*HDFS Related */
	private final String hdfsFileStr = "test/test.txt";
	private final String hdfsTestCluster = "hdfs://127.0.0.1:8020";
	
	/*
	 * Delete the test file (if the test successfully created that file)
	 */
	@After
	public void tearDown() throws IOException, URISyntaxException {
		new File(testFileStr).delete();
		hadoopCleanup();
	}
	
	private void hadoopCleanup() throws IOException, URISyntaxException{
	
		Configuration conf = new Configuration();
        conf.set("fs.defaultFS", hdfsTestCluster);

		FileSystem fs = FileSystem.get(new URI(hdfsFileStr), conf);
		fs.delete(new Path(hdfsFileStr), true);

	}
	
	/*
	 * Tests data output to a local file using a String
	 */
	@Test
	public void testOutputUsingString() throws IOException, URISyntaxException{
		DataOutput output = DataOutputFactory.getInstance().getDataOutput(factoryTestFileStr);
		
		output.outputData(testArr);
		String[] data = grabLines(new File(testFileStr));
		
		Assert.assertArrayEquals(expectedOut, data);
		
	}

	@Test
	public void testOutputUsingURL() throws IOException, URISyntaxException{
		DataOutput output = DataOutputFactory.getInstance().getDataOutput(new URL(factoryTestFileStr));
		
		output.outputData(testArr);
		String[] data = grabLines(new File(testFileStr));
		
		Assert.assertArrayEquals(expectedOut, data);
		
	}
	
	/*
	 * Tests data output to a local file using a URL
	 */
	@Test
	public void testOutputUsingURI() throws IOException, URISyntaxException{
		DataOutput output = DataOutputFactory.getInstance().getDataOutput(new URI(factoryTestFileStr));
		output.outputData(testArr);
		String[] data = grabLines(new File(testFileStr));
		
		Assert.assertArrayEquals(expectedOut, data);
		
	}
	
	/*
	 * Tests data output to a local file using a File
	 */
	@Test
	public void testOutputUsingFile() throws IOException, URISyntaxException{
		DataOutput output = DataOutputFactory.getInstance().getDataOutput(testFile);
		
		output.outputData(testArr);
		String[] data = grabLines(new File(testFileStr));
		
		Assert.assertArrayEquals(expectedOut, data);
		
	}
	
	/*
	 * Utility to pull out the data output to the file for validation
	 */
	private String[] grabLines(File file) throws IOException{
		List<String> lines = new ArrayList<String>();
		BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
		try {	
			while(br.ready()){
				lines.add(br.readLine());
			}
		} catch (IOException e) {
			
		}finally{
			br.close();
		}
		return lines.toArray(new String[lines.size()]);
	}
	
	/*
	 * Tests data output to HDFS
	 */
	
	@Test
	public void testOutputUsingHDFSString() throws IOException,
			URISyntaxException {
		// System.setProperty("HADOOP_USER_NAME", "cloudera");
		DataOutput output = DataOutputFactory.getInstance().getDataOutput(
				hdfsTestCluster + "/" + hdfsFileStr);

		output.outputData(testArr);
		String[] data = grabLinesHDFS(hdfsTestCluster, hdfsFileStr);

		Assert.assertArrayEquals(expectedOut, data);
	}

	@Test
	public void testOutputUsingHDFSURI() throws IOException, URISyntaxException {
		DataOutput output = DataOutputFactory.getInstance().getDataOutput(
				new URI(hdfsTestCluster + "/" + hdfsFileStr));

		output.outputData(testArr);
		String[] data = grabLinesHDFS(hdfsTestCluster, hdfsFileStr);

		Assert.assertArrayEquals(expectedOut, data);

	}

	@Test
	public void testOutputUsingHDFSFile() throws IOException, URISyntaxException {
		DataOutput output = null;
		output = DataOutputFactory.getInstance().getDataOutput(new File(hdfsTestCluster + "/" + hdfsFileStr));

		output.outputData(testArr);
		String[] data = grabLinesHDFS(hdfsTestCluster, hdfsFileStr);

		Assert.assertArrayEquals(expectedOut, data);
	}

	private String[] grabLinesHDFS(String hdfsLocation, String path) throws IOException, URISyntaxException{
		List<String> lines = new ArrayList<String>();

		Configuration conf = new Configuration();
        conf.set("fs.defaultFS", hdfsLocation);

		FileSystem fs = FileSystem.get(new URI(hdfsLocation), conf);

		BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(path))));
        
		while (br.ready()){
                lines.add(br.readLine());
        }
        
		return lines.toArray(new String[lines.size()]);
	}
}
