/*
 * Copyright 2015 Orient Technologies LTD (info--at--orientechnologies.com)
 * All Rights Reserved. Commercial License.
 * 
 * NOTICE:  All information contained herein is, and remains the property of
 * Orient Technologies LTD and its suppliers, if any.  The intellectual and
 * technical concepts contained herein are proprietary to
 * Orient Technologies LTD and its suppliers and may be covered by United
 * Kingdom and Foreign Patents, patents in process, and are protected by trade
 * secret or copyright law.
 * 
 * Dissemination of this information or reproduction of this material
 * is strictly forbidden unless prior written permission is obtained
 * from Orient Technologies LTD.
 * 
 * For more information: http://www.orientechnologies.com
 */

package com.orientechnologies.teleporter.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class OFileManager {
	
	public static void deleteFile(String toDeleteFilePath) {
		
		File toDeleteFile = new File(toDeleteFilePath);
		toDeleteFile.delete();
		
	}

	public static void extractAll(String inputArchiveFilePath, String outputFolderPath) throws IOException {

		File inputArchiveFile = new File(inputArchiveFilePath);
		if (!inputArchiveFile.exists()) {
			throw new IOException(inputArchiveFile.getAbsolutePath() + " does not exist");
		}

		// distinguishing archive format
		//		TODO

		// Extracting zip file
		try {
			unZipAll(inputArchiveFile, outputFolderPath);
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	public static void unZipAll(File inputZipFile, String destinationFolderPath) throws IOException {

		byte[] buffer = new byte[1024];

		try{

			//get the zip file content
			ZipInputStream zipInputStream = new ZipInputStream(new FileInputStream(inputZipFile));
			//get the zipped file list entry
			ZipEntry zipEntry = zipInputStream.getNextEntry();

			while(zipEntry != null){

				String fileName = zipEntry.getName();
				String newFilePath = destinationFolderPath + File.separator + fileName;
				File newFile = new File(newFilePath);

				FileOutputStream fileOutputStream = null;

				// if the entry is a file, extracts it
				if (!zipEntry.isDirectory()) {
					fileOutputStream = new FileOutputStream(newFile); 
					int len;
					while ((len = zipInputStream.read(buffer)) > 0) {
						fileOutputStream.write(buffer, 0, len);
					}

				} else {
					// if the entry is a directory, make the directory
					File dir = new File(newFilePath);
					dir.mkdir();
				}

				if(fileOutputStream != null)
					fileOutputStream.close();   
				zipEntry = zipInputStream.getNextEntry();

			}

			zipInputStream.closeEntry();
			zipInputStream.close();

		} catch(IOException ex){
			ex.printStackTrace(); 
		}
	}    

}
