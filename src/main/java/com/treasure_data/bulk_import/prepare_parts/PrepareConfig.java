package com.treasure_data.bulk_import.prepare_parts;

import java.util.Properties;

import com.treasure_data.bulk_import.Config;

public class PrepareConfig extends Config {

    // FIXME this field is also declared in td-client.Config.
    protected Properties props;

    public PrepareConfig() {
    }

    public void configure(Properties props) {
        this.props = props;
    }
}
