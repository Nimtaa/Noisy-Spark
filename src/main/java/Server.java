import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.sql.Timestamp;

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
        System.out.println(Timestamp.valueOf("2018-08-27 12:14:18"));
        Server s = new Server(9998);
    }



}
