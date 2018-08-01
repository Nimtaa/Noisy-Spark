import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.Random;

public class Client {

    private Socket socket = null;
    private DataOutputStream out  = null;

    public Client (String address,int port){

        try {

            socket = new Socket(address,port);
            System.out.println("connected");
            out = new DataOutputStream(socket.getOutputStream());

            while(true) {
                //out.writeInt((randomNumberGenerator()));
                out.writeInt(randomNumberGenerator());
                System.out.println("wrote to output");
                socket.setSoTimeout(1000);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

    }
    public static void main(String[] args) {
       Client s = new Client("127.0.0.1",9998);
    }

    static int randomNumberGenerator(){
        Random rand = new Random();
        return rand.nextInt(9999)+1000;
    }

}
