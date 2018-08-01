import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.Socket;
import java.util.Random;

public class Client {

    private Socket socket = null;
    private DataOutputStream out  = null;
    private OutputStreamWriter osw = null;
    public Client (String address,int port){

        try {

            socket = new Socket(address,port);
            System.out.println("connected");
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
       Client s = new Client("127.0.0.1",9998);
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
