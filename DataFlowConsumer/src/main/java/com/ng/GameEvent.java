package com.ng;


import java.io.Serializable;
import java.util.Date;
import java.util.UUID;

public class GameEvent implements Serializable {

    private UUID gameId;
    private Date creationDate;

    public GameEvent() {
    }

    public UUID getGameId() {
        return gameId;
    }

    public void setGameId(UUID gameId) {
        this.gameId = gameId;
    }

    public Date getCreationDate() {
        return creationDate;
    }

    public void setCreationDate(Date creationDate) {
        this.creationDate = creationDate;
    }
}
