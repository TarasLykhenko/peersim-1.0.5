package example.myproject.datatypes;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MessageBuffer {

    Map<Long, List<Message>> messageBuffer = new HashMap<>();

    public void bufferMessage(Message message) {
        this.messageBuffer
                .computeIfAbsent(message.getForwarder(), k -> new ArrayList<>())
                .add(message);
    }

    // TODO hardcore todo
    public List<Message> checkBufferedMessages(Message receivedMessage) {
        Long forwarder = receivedMessage.getForwarder();

        List<Message> bufferedMessages = messageBuffer.get(forwarder);

        if (bufferedMessages.isEmpty()) {
            return Collections.emptyList();
        }

        Message firstBufferedMessage = bufferedMessages.get(0);

        //TODO verificação
        // Se consegue processar a 1º mensagem então deve conseguir processar todas as seguintes, devido a FIFO
        List<Message> result = new ArrayList<>(bufferedMessages);
        messageBuffer.get(forwarder).clear();
        return result;
    }

    public List<Message> processMessages(Message message) {
        return null;
    }
}
