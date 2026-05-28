package com.commercial.cards.base24.mapper;

import com.commercial.cards.base24.model.Base24Message;
import com.commercial.cards.base24.orchestration.EventMapper;
import com.commercial.cards.base24.parser.Base24XmlParser;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.util.Optional;

@Component
@RequiredArgsConstructor
public class Base24EventMapper implements EventMapper<String, Base24Message> {

    private final Base24XmlParser parser;

    @Override
    public Optional<Base24Message> map(String input) {
        return parser.parse(input);
    }
}
