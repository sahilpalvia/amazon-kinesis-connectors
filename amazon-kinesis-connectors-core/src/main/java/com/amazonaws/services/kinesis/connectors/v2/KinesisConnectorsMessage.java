package com.amazonaws.services.kinesis.connectors.v2;

import lombok.Getter;

/**
 *
 */
public class KinesisConnectorsMessage {
    @Getter
    private int userid;
    @Getter
    private String username;
    @Getter
    private String firstname;
    @Getter
    private String lastname;
    @Getter
    private String city;
    @Getter
    private String state;
    @Getter
    private String email;
    @Getter
    private String phone;
    @Getter
    private boolean likesSports;
    @Getter
    private boolean likesTheatre;
    @Getter
    private boolean likesConcerts;
    @Getter
    private boolean likesJazz;
    @Getter
    private boolean likesClassical;
    @Getter
    private boolean likesOpera;
    @Getter
    private boolean likesRock;
    @Getter
    private boolean likesVegas;
    @Getter
    private boolean likesBroadway;
    @Getter
    private boolean likesMusicals;

    public KinesisConnectorsMessage() {
        // Default constructor
    }

    public KinesisConnectorsMessage(final int userid,
                                    final String username,
                                    final String firstname,
                                    final String lastname,
                                    final String city,
                                    final String state,
                                    final String email,
                                    final String phone,
                                    final boolean likesSports,
                                    final boolean likesTheatre,
                                    final boolean likesConcerts,
                                    final boolean likesJazz,
                                    final boolean likesClassical,
                                    final boolean likesOpera,
                                    final boolean likesRock,
                                    final boolean likesVegas,
                                    final boolean likesBroadway,
                                    final boolean likesMusicals) {
        this.userid = userid;
        this.username = username;
        this.firstname = firstname;
        this.lastname = lastname;
        this.city = city;
        this.state = state;
        this.email = email;
        this.phone = phone;
        this.likesSports = likesSports;
        this.likesTheatre = likesTheatre;
        this.likesConcerts = likesConcerts;
        this.likesJazz = likesJazz;
        this.likesClassical = likesClassical;
        this.likesOpera = likesOpera;
        this.likesRock = likesRock;
        this.likesVegas = likesVegas;
        this.likesBroadway = likesBroadway;
        this.likesMusicals = likesMusicals;
    }
}
