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
package org.finra.datagenerator.factory;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;

import org.finra.datagenerator.impl.HDFSFileOutput;
import org.finra.datagenerator.impl.LocalFileOutput;
import org.finra.datagenerator.interfaces.DataOutput;

/**
 * Factory class that grabs the corresponding DataOutput class based on the output file passed in
 * @author MeleS
 *
 */
public class DataOutputFactory {
		
	private static DataOutputFactory instance;
	
	public static DataOutputFactory getInstance(){
		if(instance == null){
			instance = new DataOutputFactory();
		}
		
		return instance;
	}
	/**
	 *
	 * Returns a DataOutput file based on the filePath you pass in
	 * @param filePath
	 * @return
	 * @throws IOException 
	 * @throws URISyntaxException 
	 */
	public DataOutput getDataOutput(String filePath) throws IOException, URISyntaxException{
		return getDataOutput(new URI(filePath));
	}
	
	/**
	 * get the data output based on what you pass in through a File object, the filename must start with file:// or hdfs://
	 * @param theFile
	 * @return
	 * @throws IOException
	 * @throws URISyntaxException
	 */
	
	public DataOutput getDataOutput(File theFile) throws IOException, URISyntaxException{
		if(theFile.toString().startsWith("file")){
			if(File.separator.equals("\\")){
				return getDataOutput(new URL(normalizeFileName(theFile.getPath())));
			}else{
				return getDataOutput(new URL(theFile.getPath().replaceFirst("/", "//")));	
			}
		}else if(theFile.toString().startsWith("hdfs")){
			if(File.separator.equals("\\")){
				return getDataOutput(new URI(normalizeFileName(theFile.getPath())));			
			}else{
				return getDataOutput(new URI(theFile.getPath().replaceFirst("/", "//")));				
			}
		}else{
			throw new IOException("Unable to identify the resource type (" + theFile.toString() + ")");
		}
	}

	/*
	 * Private method to convert windows file separators to unix style file separators
	 */
	private String normalizeFileName(String fileName){
		return fileName.replaceFirst("\\\\", "//").replaceAll("\\\\", "/");
	}

	/**
	 * Returns a DataOutput object based on the URL you pass in
	 * @param theURL
	 * @return
	 * @throws IOException
	 * @throws URISyntaxException
	 */
	public DataOutput getDataOutput(URL theURL) throws IOException, URISyntaxException{
		return getDataOutput(theURL.toURI());
	}
	
	/**
	 * Returns a DataOutput object based on the URI you pass in
	 * @param uri
	 * @return
	 * @throws IOException
	 */
	public DataOutput getDataOutput(URI uri) throws IOException{
		if(uri.toString().startsWith("file://")){
			return new LocalFileOutput(uri);
		}else if(uri.toString().startsWith("hdfs://")){
			return new HDFSFileOutput(uri);
		}else{
			throw new IOException("Unable to identify the resource type (" + uri.toString() + ")");
		}
	}
	
}
