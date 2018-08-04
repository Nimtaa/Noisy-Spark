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
            dos = new DataOutputStream(socket.getOutputStream());
            while(true) {
                dos.writeBytes(randomStringGenerator()+" ");
                server.setSoTimeout(750);
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
        return rand.nextInt(9999)+1000;
    }
    static String randomStringGenerator(){
        String [] arr =  {"ali","hi","hasan","Hamid","World","spark"};
        int index = new Random().nextInt(5)+0;
        return arr[index];
    }
}
