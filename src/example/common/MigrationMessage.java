package example.common;

import peersim.core.CommonState;

public class MigrationMessage {
    public final long senderDC;
    public final int clientId;
    public final long timestamp;

    public MigrationMessage(long senderDC, int clientId) {
        this.senderDC = senderDC;
        this.clientId = clientId;
        timestamp = CommonState.getTime();
    }
}
