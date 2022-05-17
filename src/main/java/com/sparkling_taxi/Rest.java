package com.sparkling_taxi;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;

public class Rest {

    public static void get(String urlString, String query) throws IOException {
        URL url = new URL(urlString);

        //make connection
        URLConnection urlConnection = url.openConnection();

        //use post mode
        urlConnection.setDoOutput(true);
        urlConnection.setAllowUserInteraction(false);

        //send query
        PrintStream ps = new PrintStream(urlConnection.getOutputStream());
        ps.print(query);
        ps.close();

        //get result
        BufferedReader br = new BufferedReader(new InputStreamReader(urlConnection.getInputStream()));
        String l = null;
        while ((l=br.readLine())!=null) {
            System.out.println(l);
        }
        br.close();
    }
}
