//new
package it.univr.auditel.entities;

import java.io.Serializable;

/**
 * It represents the transition preference between two channels
 *
 * @author Mattia Carra
 * @version 0.0.0
 */
public class ChannelTransition implements Serializable{

    private String channelId1;
    private String channelId2;
    private Double preference_transition;

    public ChannelTransition(){
        channelId1 = null;
        channelId2 = null;
        preference_transition = null;
    }

    public String getChannelId1() {
        return channelId1;
    }

    public void setChannelId1( String channelId1 ) {
        this.channelId1 = channelId1;
    }

    public String getChannelId2() {
        return channelId2;
    }

    public void setChannelId2( String channelId2 ) {
        this.channelId2 = channelId2;
    }

    public Double getPreferenceTransition() {
        return preference_transition;
    }

    public void setPreferenceTransition( Double preference_transition ) {
        this.preference_transition = preference_transition;
    }
}
//newend
