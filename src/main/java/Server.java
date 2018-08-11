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

            try {
                server = new ServerSocket(port);
            } catch (IOException e) {
                e.printStackTrace();
            }
            System.out.println("waiting for client...");
            while (true) {
                socket = server.accept();

                Runnable connectionHanlder = new ConnectionHandler(socket);
                System.out.println("client connected");
                new Thread(connectionHanlder).start();

//            dos = new DataOutputStream(socket.getOutputStream());
//            while(true) {
//                //dos.writeBytes(randomStringGenerator()+" ");
//                dos.writeBytes(randomCity()+","+ randomNumberGenerator()+"\n");
//                server.setSoTimeout(100);
//            }
//

            }
        } catch (IOException e) {
            e.printStackTrace();
        }


    }
    public static void main(String[] args) {
        Server s = new Server(9998);
    }



}
