/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.kafka.schema.confluent;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;

import java.util.Map;

import static io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig.BASIC_AUTH_CREDENTIALS_SOURCE;
import static io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig.USER_INFO_CONFIG;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class SchemaRegistryClientBasicAuthProvider
        implements SchemaRegistryClientPropertiesProvider
{
    private final String userInfo;

    @Inject
    public SchemaRegistryClientBasicAuthProvider(ConfluentSchemaRegistryConfig config)
    {
        requireNonNull(config, "config is null");
        String username = requireNonNull(
                config.getAuthenticationUsername(),
                format("'kafka.confluent-schema-registry.authentication.username' must be set when authentication type is %s", config.getAuthenticationType()));
        String password = requireNonNull(
                config.getAuthenticationPassword(),
                format("'kafka.confluent-schema-registry.authentication.password' must be set when authentication type is %s", config.getAuthenticationType()));
        this.userInfo = username + ":" + password;
    }

    @Override
    public Map<String, Object> getSchemaRegistryClientProperties()
    {
        return ImmutableMap.of(
                BASIC_AUTH_CREDENTIALS_SOURCE, "USER_INFO",
                USER_INFO_CONFIG, userInfo);
    }
}
