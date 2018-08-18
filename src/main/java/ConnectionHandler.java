import java.io.*;
import java.net.Socket;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;


public class ConnectionHandler implements Runnable{

    private DataOutputStream dos = null;
    private Socket socket;
    static  int counter ;
    public  ConnectionHandler(Socket socket){
        this.socket = socket;

    }
    @Override
    public void run() {
        counter++;
        try {
            PrintWriter pw = new PrintWriter(new File("/home/nima/Desktop/tempDir/outfile" + counter + ".csv"));
            StringBuilder sb = new StringBuilder();
            sb.append("city");
            sb.append(',');
            sb.append("temperature");
            sb.append('\n');
            pw.write(sb.toString());


            try {
                dos = new DataOutputStream(socket.getOutputStream());
            } catch (IOException e) {
                e.printStackTrace();
            }
            while (true) {
//            try {
                    dos.writeBytes(currentTimestamp()+","+randomCity()+","+ randomNumberGenerator() +"\n");
//                pw.write(randomCity() + "," + randomNumberGenerator() + "\n");
//                pw.flush();
//                Thread.sleep(50);
            }
                    //server.setSoTimeout(100);
//            try {
//
//                socket.setSoTimeout(1000);
//            } catch (SocketException e) {
//                e.printStackTrace();
//            }



            }  catch (FileNotFoundException e1) {
            e1.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }


    }

    static int randomNumberGenerator(){
        Random rand = new Random();
        return rand.nextInt(50)+15;
    }

    static String randomStringGenerator(){
        String [] arr =  {"ali","hi","hasan","Hamid","World","spark"};
        int index = new Random().nextInt(5)+0;
        return arr[index];
    }

    //random city picker for sending structured stream message to spark city,temperature
    static String randomCity(){

        String [] cities = {"Tirane", "Andorra-la-vella", "Jerevan", "Vienna", "Baku",
                "Minsk", "Brussels", "Sarajevo", "Sofia", "Zagreb",
                "Nicosia", "Prague", "Copenhagen", "Tallinn", "Helsinki",
                "Paris", "Cayenne", "Tbilisi", "Berlin", "Athens",
                "Budapest", "Reykjavik", "Rome", "Riga", "Vaduz",
                "Vilnius", "Luxemburg", "Skopje", "Valletta", "Fort-de-France",
                "Kishinev", "Monaco", "Amsterdam", "Oslo", "Belfast",
                "Warsaw", "Lisbon", "Bucharest", "Moscow", "San Marino",
                "Edinburgh", "Bratislava", "Ljubljana", "Madrid", "Stockholm",
                "Berne", "Dushanbe", "Kiev", "London", "Toshkent",
                "Vatican City", "Belgrade"};

        int index = new Random().nextInt(cities.length-1)+0;
        return cities[index];


    }

    static String currentTimestamp(){
        Timestamp ts = new Timestamp(System.currentTimeMillis());
        SimpleDateFormat s =  new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
        return s.format(ts.getTime());

    }

    public static void main(String[] args) {

        System.out.println(currentTimestamp());
    }
}
