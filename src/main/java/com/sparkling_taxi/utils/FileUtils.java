package com.sparkling_taxi.utils;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.json.JSONException;
import org.json.JSONObject;


import java.io.*;
import java.net.URI;
import java.net.URL;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.logging.Logger;
import java.util.stream.Stream;

import static com.sparkling_taxi.utils.Const.LINUX_SEPARATOR;
import static com.sparkling_taxi.utils.Const.WINDOWS_SEPARATOR;

public class FileUtils {
    private static final Logger logger = Logger.getLogger(FileUtils.class.getName());

    private FileUtils() {
    }

    public static boolean isDirEmpty(File directory) {
        Path path = directory.toPath();
        if (Files.isDirectory(path)) {
            try (Stream<Path> entries = Files.list(path)) {
                return !entries.findFirst().isPresent();
            } catch (IOException io) {
                logger.severe("Error while listing entries in folder: " + directory + "\n" + io.getMessage());
            }
        }
        return false;
    }

    /**
     * Returns the extension without "."
     */
    public static String getExtensionWithoutPoint(String filename) {
        return Optional.ofNullable(filename)
                .filter(f -> f.contains("."))
                .map(f -> f.substring(filename.lastIndexOf(".") + 1)).orElse("");
    }

    /**
     * Returns the extension of the input file with "."
     *
     * @param filename
     * @return extension with "."
     */
    public static String getExtension(String filename) {
        return "." + getExtensionWithoutPoint(filename);
    }

    public static String getFileNameWithoutExtension(String fileNameWithExtension) {
        if (!fileNameWithExtension.contains(".")) return fileNameWithExtension;
        return fileNameWithExtension.substring(0, getStartIndexOfExtension(fileNameWithExtension) - 1);
    }

    /**
     * @param path the path of a file
     * @return the filename with the extension
     */
    public static String getFilenameFromPath(String path) {
        String sep = LINUX_SEPARATOR;
        if (Utils.isWindows()) {
            sep = WINDOWS_SEPARATOR;
        }
        List<String> pathPieces = Arrays.asList(path.split(sep));
        if (!pathPieces.isEmpty()) {
            return pathPieces.get(pathPieces.size() - 1);
        }
        return "";
    }

    public static String getFilenameFromPathWithoutExtension(String path) {
        return getFileNameWithoutExtension(getFilenameFromPath(path));
    }

    private static int getStartIndexOfExtension(String fileNameWithExtension) {
        char[] charArray = fileNameWithExtension.toCharArray();
        int startIndexOfExtension = 0;
        for (int i = charArray.length - 1; i >= 0; i--) {
            char c = charArray[i];
            if (c == '.') {
                startIndexOfExtension = i + 1;
                break;
            }
        }
        return startIndexOfExtension;
    }

    /**
     * Change extension of a file, if present, otherwise adds the specified extension
     *
     * @param fileName     only the file name, including the extension.
     * @param newExtension the new extension. It can have the "." but it is not necessary
     * @return a new String with old name and new extension
     */
    public static String changeExtension(String fileName, String newExtension) {
        if (newExtension.contains(".")) return getFileNameWithoutExtension(fileName) + newExtension;
        else if (newExtension.equals("")) return getFileNameWithoutExtension(fileName);
        return getFileNameWithoutExtension(fileName) + "." + newExtension;
    }

    public static String appendEndOfFileName(String oldName, String append) {
        if (oldName.contains(".") && append.contains("."))
            throw new IllegalArgumentException("la stringa da aggiungere non può contenere un punto se questo è già presente nel nome iniziale");
        else if (oldName.contains(".") && !append.contains("."))
            return getFileNameWithoutExtension(oldName) + append + getExtension(oldName);
        else return oldName + append;
    }

    /**
     * Checks if two file have the same extension
     *
     * @param file1     pathName with extension of the first file
     * @param file2     pathName with extension of the first file
     * @param extension the extension to check (with or without ".")
     * @return true, if the two files have the same extension.
     */
    public static boolean sameExtension(String file1, String file2, String extension) {
        if (extension.charAt(0) == '.') {
            return extension.equals(getExtension(file1)) && extension.equals(getExtension(file2));
        }
        return extension.equals(getExtensionWithoutPoint(file1)) && extension.equals(getExtensionWithoutPoint(file2));
    }

    public static boolean isJavaFile(String fileName) {
        return getExtensionWithoutPoint(fileName).equals("java");
    }

    /**
     * Useful for RestQueries
     *
     * @param url url with Rest json result
     * @return json object with query result
     * @throws IOException
     * @throws JSONException
     */
    public static JSONObject readJsonFromUrl(String url) throws IOException, JSONException {
        try (InputStream is = new URL(url).openStream()) {
            BufferedReader rd = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder();
            int cp;
            while ((cp = rd.read()) != -1) {
                sb.append((char) cp);
            }
            return new JSONObject(sb.toString());
        } catch (UnknownHostException unk) {
            logger.severe("Impossibile collegarsi a internet...");
            return null;
        }
    }

    /**
     * Used to get a List from an array represented by the string in input.
     * Parses a string which represents a Java array, and converts it in a List of strings
     *
     * @param listString the string obtained from Arrays.toString(T[])
     * @return List<String> a list of strings with contents of the array
     */
    public static List<String> readListFromArrayString(String listString) {
        if (listString.equals("[]")) {
            return new ArrayList<>();
        }
        String substring = listString.substring(1, listString.length() - 1); // Tolgo le [ ]
        String[] split = substring.split(", ");
        return Arrays.asList(split);
    }

    /**
     * Check if a HDFS file exists
     *
     * @param file hdfs path of the file
     * @return true if file exists
     */
    public static boolean hasFileHDFS(String file) {
        try {
            Configuration conf = new Configuration();
            conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
            conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
            //Get the filesystem - HDFS//Get the filesystem - HDFS
            FileSystem hdfs = FileSystem.get(URI.create(file), conf);
            if (hdfs.exists(new org.apache.hadoop.fs.Path(file))) return true;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    /**
     * @param dir     name of directory
     * @param pattern a lambda that returns a boolean
     * @return an optional string with the first file that matches the pattern
     */
    public static Optional<String> getFirstFileWithPattern(String dir, FilenameFilter pattern) {
        File f = new File(dir);
        if (f.exists()) {
            return Arrays.stream(Objects.requireNonNull(f.listFiles(pattern)))
                    .map(File::getName)
                    .findFirst();
        }
        return Optional.empty();
    }

    /**
     * Rename File
     *
     * @param parentPath path of parent dir
     * @param name the old name
     * @param newName    the new name
     * @return true if all goes well
     */
    public static boolean renameFile(String parentPath, String name, String newName) {
        File file = new File(parentPath + name);
        // Create an object of the File class
        // Replace the file path with path of the directory
        File rename = new File(parentPath + newName);
        // store the return value of renameTo() method in
        // flag
        return file.renameTo(rename);
    }

    /**
     * @param dir     name of directory
     * @param startWith a lambda that returns a boolean
     * @return an optional string with the first file that matches the pattern
     */
    public static Optional<String> getFirstFileStartWithHDFS(String dir, String startWith) {
        try {
            Configuration conf = new Configuration();
            conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
            conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
            //Get the filesystem - HDFS//Get the filesystem - HDFS
            FileSystem hdfs = FileSystem.get(URI.create(dir), conf);
            FileStatus[] status = hdfs.listStatus(   new org.apache.hadoop.fs.Path(dir));
            for (FileStatus fileStatus : status) {
                String name = fileStatus.getPath().getName();
                if (name.startsWith(startWith)){
                    return Optional.of(name);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return Optional.empty();
    }

    public static boolean copyFromHDFS(String from, String to){
        System.out.println("from = " + from + ", to = " + to);
        try {
            Configuration conf = new Configuration();
            conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
            conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
            //Get the filesystem - HDFS//Get the filesystem - HDFS
            FileSystem hdfs = FileSystem.get(URI.create(from), conf);
            if (hdfs.exists(new org.apache.hadoop.fs.Path(from))){
                System.out.println("Copying");
                hdfs.copyToLocalFile(false, new org.apache.hadoop.fs.Path(from),new org.apache.hadoop.fs.Path(to), true);
                return true;
            } else {
                System.out.println("AIUTO non esisto");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("QUI NON VA BENISSIMO");
        return false;
    }

    public static boolean deleteFromHDFS(String file){
        try {
            Configuration conf = new Configuration();
            conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
            conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
            //Get the filesystem - HDFS//Get the filesystem - HDFS
            FileSystem hdfs = FileSystem.get(URI.create(file), conf);
            if (hdfs.exists(new org.apache.hadoop.fs.Path(file))){
                System.out.println("Copying");
                hdfs.delete(new org.apache.hadoop.fs.Path(file), true);
                return true;
            } else {
                System.out.println("AIUTO esisto");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("E QUI NON VA BENISSIMO");
        return false;
    }

}
