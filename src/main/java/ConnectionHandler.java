import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.net.SocketException;
import java.util.Random;

public class ConnectionHandler implements Runnable{

    private DataOutputStream dos = null;
    private Socket socket;

    public  ConnectionHandler(Socket socket){
        this.socket = socket;

    }
    @Override
    public void run() {
        try {
            dos = new DataOutputStream(socket.getOutputStream());
        } catch (IOException e) {
            e.printStackTrace();
        }
        while(true) {
            try {
                dos.writeBytes(randomCity()+","+ randomNumberGenerator()+"\n");
            } catch (IOException e1) {
                e1.printStackTrace();
            }
            //server.setSoTimeout(100);
            try {

                socket.setSoTimeout(500);
            } catch (SocketException e) {
                e.printStackTrace();
            }
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


}
