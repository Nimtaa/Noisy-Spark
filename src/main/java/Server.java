import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Random;

public class Server {

    private Socket socket = null;
    private ServerSocket server = null;
    private DataOutputStream dos = null;

    public Server(int port){

        try {
            server = new ServerSocket(port);
            System.out.println("waiting for client...");
            socket = server.accept();
            System.out.println("connected!!");
            dos = new DataOutputStream(socket.getOutputStream());
            while(true) {
                //dos.writeBytes(randomStringGenerator()+" ");
                dos.writeBytes(randomCity()+","+ randomNumberGenerator()+"\n");
                server.setSoTimeout(100);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

    }
    public static void main(String[] args) {
        Server s = new Server(9998);
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
