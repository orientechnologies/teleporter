/*
 * Copyright 2015 OrientDB LTD (info--at--orientdb.com)
 * All Rights Reserved. Commercial License.
 * 
 * NOTICE:  All information contained herein is, and remains the property of
 * OrientDB LTD and its suppliers, if any.  The intellectual and
 * technical concepts contained herein are proprietary to
 * OrientDB LTD and its suppliers and may be covered by United
 * Kingdom and Foreign Patents, patents in process, and are protected by trade
 * secret or copyright law.
 * 
 * Dissemination of this information or reproduction of this material
 * is strictly forbidden unless prior written permission is obtained
 * from OrientDB LTD.
 * 
 * For more information: http://www.orientdb.com
 */

package com.orientechnologies.teleporter.util;

import com.orientechnologies.orient.core.record.impl.ODocument;

import java.io.*;
import java.nio.charset.Charset;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class OFileManager {

  public static void deleteResource(String resourcePath) throws IOException {

    File currentFile = new File(resourcePath);
    if (currentFile.isDirectory()) {
      File[] innerFiles = currentFile.listFiles();
      for (File file : innerFiles) {
        deleteResource(file.getCanonicalPath());
      }
    }
    if (!currentFile.delete())
      throw new IOException();
  }

  public static void extractAll(String inputArchiveFilePath, String outputFolderPath) throws IOException {

    File inputArchiveFile = new File(inputArchiveFilePath);
    if (!inputArchiveFile.exists()) {
      throw new IOException(inputArchiveFile.getAbsolutePath() + " does not exist");
    }

    // distinguishing archive format
    //		TODO

    // Extracting zip file
    unZipAll(inputArchiveFile, outputFolderPath);

  }

  public static void unZipAll(File inputZipFile, String destinationFolderPath) throws IOException {

    byte[] buffer = new byte[1024];

    //get the zip file content
    ZipInputStream zipInputStream = new ZipInputStream(new FileInputStream(inputZipFile));
    //get the zipped file list entry
    ZipEntry zipEntry = zipInputStream.getNextEntry();

    while (zipEntry != null) {

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

      if (fileOutputStream != null)
        fileOutputStream.close();
      zipEntry = zipInputStream.getNextEntry();

    }

    zipInputStream.closeEntry();
    zipInputStream.close();

  }

  public static String readAllTextFile(Reader rd) throws IOException {
    StringBuilder sb = new StringBuilder();
    int cp;
    while ((cp = rd.read()) != -1) {
      sb.append((char) cp);
    }
    return sb.toString();
  }

  /**
   * It returns a ODocument starting from a json file.
   *
   * @param filePath
   *
   * @return ODocument (null if the file does not exist or problem are encountered during the reading)
   */

  public static ODocument buildJsonFromFile(String filePath) throws IOException {

    if (filePath == null) {
      return null;
    }

    File jsonFile = new File(filePath);
    if (!jsonFile.exists()) {
      return null;
    }

    FileInputStream is = new FileInputStream(jsonFile);
    BufferedReader rd = new BufferedReader(new InputStreamReader(is, Charset.forName("UTF-8")));
    ODocument json = new ODocument();
    String jsonText = OFileManager.readAllTextFile(rd);
    json.fromJSON(jsonText, "noMap");
    return json;

  }

  public static void writeFileFromText(String text, String outFilePath, boolean append) throws IOException {

    File outFile = new File(outFilePath);
    outFile.getParentFile().mkdirs();
    outFile.createNewFile();
    PrintWriter out = new PrintWriter(new FileWriter(outFile, append));
    out.println(text);
    out.close();
  }
}
