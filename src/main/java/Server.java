import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Random;

public class Server {

    private Socket socket = null;
    private OutputStreamWriter osw = null;
    private ServerSocket server = null;

    public Server(int port){

        try {

            server = new ServerSocket(port);

            System.out.println("waiting for client...");
            socket = server.accept();

            osw = new OutputStreamWriter(socket.getOutputStream(), "UTF-8");

            while(true) {
                osw.write(randomStringGenerator()+" ");
                System.out.println("wrote to output");
                server.setSoTimeout(1000);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

    }
    public static void main(String[] args) {
       //Server s = new Server("127.0.0.1",9999);
        Server s = new Server(9999);
    }

    static int randomNumberGenerator(){
        Random rand = new Random();
        return rand.nextInt(9999)+1000;
    }
    static String randomStringGenerator(){
        String [] arr =  {"ali","hi","hasan","Hamid","World","spark"};
        int index = new Random().nextInt(5)+0;
        return arr[index];
    }
}
