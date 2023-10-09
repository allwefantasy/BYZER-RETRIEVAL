package tech.mlsql.retrieval;

import io.ray.api.Ray;
import io.ray.api.options.ActorLifetime;
import io.ray.api.runtimeenv.RuntimeEnv;
import io.ray.api.runtimeenv.types.RuntimeEnvName;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 10/9/23 WilliamZhu(allwefantasy@gmail.com)
 */
public class RetrievalGatewayLauncher {
    public RetrievalGatewayLauncher() {

    }

    public boolean launch() {
        var runtimeEnv = new RuntimeEnv.Builder().build();
        Map<String, String> envMap = new HashMap<>();
        System.getenv().forEach((k, v) -> {
            System.out.println(k + " " + v);
            envMap.put(k, v);
        });
        
        runtimeEnv.set(RuntimeEnvName.ENV_VARS, envMap);
        Ray.actor(RetrievalGateway::new).
                setName("RetrievalGateway").
                setLifetime(ActorLifetime.DETACHED).
                setRuntimeEnv(runtimeEnv).
                setJvmOptions(Utils.defaultJvmOptions()).
                remote();
        return true;
    }
}
