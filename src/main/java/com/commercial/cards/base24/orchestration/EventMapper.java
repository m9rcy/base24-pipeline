package com.commercial.cards.base24.orchestration;

import java.util.Optional;

public interface EventMapper<I, D> {

    Optional<D> map(I input);
}
