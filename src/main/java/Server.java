import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Random;

public class Server {

    private Socket socket = null;
    private DataOutputStream out  = null;
    private OutputStreamWriter osw = null;

    private ServerSocket server = null;
    public Server(int port){

        try {

            //socket = new Socket(address,port);
            server = new ServerSocket(port);

            System.out.println("waiting for client...");
            socket = server.accept();

            out = new DataOutputStream(socket.getOutputStream());
            osw = new OutputStreamWriter(socket.getOutputStream(), "UTF-8");

            while(true) {
                //out.writeInt(randomNumberGenerator());
                //out.writeUTF(randomStringGenerator());
                osw.write(randomStringGenerator()+" ");
                System.out.println("wrote to output");
                socket.setSoTimeout(1000);
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
