package com.commercial.cards.base24.config;

import com.commercial.cards.base24.dedupe.EventDeduplicationRepository;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.flyway.FlywayAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.autoconfigure.orm.jpa.HibernateJpaAutoConfiguration;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.transaction.annotation.EnableTransactionManagement;

import javax.sql.DataSource;

@Configuration
@ConditionalOnProperty(prefix = "base24.dedupe", name = "enabled", havingValue = "true")
// Import order: DataSource first (Hibernate needs it), Hibernate second (creates JpaTransactionManager),
// Flyway last (runs migrations after EMF is ready). TransactionAutoConfiguration is replaced by the
// explicit @EnableTransactionManagement below to avoid its @ConditionalOnSingleCandidate timing issue.
@Import({
    DataSourceAutoConfiguration.class,
    HibernateJpaAutoConfiguration.class,
    FlywayAutoConfiguration.class
})
@EnableJpaRepositories(basePackageClasses = EventDeduplicationRepository.class)
@EnableTransactionManagement
public class DedupeDataSourceConfig {

    @Bean
    @ConditionalOnMissingBean
    public DataSource dataSource(
            @Value("${spring.datasource.url}") String url,
            @Value("${spring.datasource.username}") String username,
            @Value("${spring.datasource.password}") String password
    ) {
        return DataSourceBuilder.create()
                .url(url)
                .username(username)
                .password(password)
                .build();
    }
}
