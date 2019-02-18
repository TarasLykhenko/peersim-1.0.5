package example.common;

import peersim.core.CommonState;

public class CommonMessages {
    class MigrationMessage {
        final long senderDC;
        final int clientId;
        final long timestamp;

        MigrationMessage(long senderDC, int clientId) {
            this.senderDC = senderDC;
            this.clientId = clientId;
            timestamp = CommonState.getTime();
        }
    }
}
