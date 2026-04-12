package com.commercial.cards.base24.port;

public interface TokenisationPort {

    /**
     * Tokenise a digital PAN by calling the cards-tokenisation-service.
     *
     * @param digitalPan the raw digital PAN to tokenise
     * @return the tokenised PAN to use downstream
     * @throws com.commercial.cards.base24.exception.TokenisationException on failure
     */
    String tokenise(String digitalPan);
}
