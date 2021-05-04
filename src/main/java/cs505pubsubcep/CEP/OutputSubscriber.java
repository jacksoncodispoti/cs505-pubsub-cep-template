package cs505pubsubcep.CEP;

import io.siddhi.core.util.transport.InMemoryBroker;
import cs505pubsubcep.Launcher;

public class OutputSubscriber implements InMemoryBroker.Subscriber {

    private String topic;

    public OutputSubscriber(String topic, String streamName) {
        this.topic = topic;
    }

    @Override
    public void onMessage(Object msg) {

        try {
            System.out.println("OUTPUT CEP EVENT: " + msg);
            System.out.println("");
            String mes = String.valueOf(msg);
            if(mes.charAt(0)=='{')
                mes = "[" + mes;
            Launcher.messageCounter++;
            if(Launcher.messageCounter%2==1)
                Launcher.message30 = mes;
            else {
                for(int i=0;i<Launcher.isInAlert.length;i++)
                    Launcher.isInAlert[i] = false;
                for(int i=24;i<mes.length();i+=41) {
                    int zip = Integer.parseInt(mes.substring(i,i+4));
                    for(int j=24;j<Launcher.message30.length();j+=41) {
                        if(zip==Integer.parseInt(Launcher.message30.substring(j,j+4))) {
                            int i_end = 15;
                            int j_end = 15;
                            while(mes.charAt(i+i_end)!='}')
                                i_end++;
                            while(Launcher.message30.charAt(j+j_end)!='}')
                                j_end++;
                             if(Integer.parseInt(mes.substring(i+14,i+i_end)) / 2.0 * 3.0 >= Integer.parseInt(Launcher.message30.substring(j+14,j+j_end)))
                                 Launcher.isInAlert[zip]=true;
                        }
                    }
                }
                for(int i=0;i<Launcher.isInAlert.length;i++)
                    if(Launcher.isInAlert[i])
                        System.out.println(40000 + i);
            }

        } catch(Exception ex) {
            ex.printStackTrace();
        }

    }

    @Override
    public String getTopic() {
        return topic;
    }

}
