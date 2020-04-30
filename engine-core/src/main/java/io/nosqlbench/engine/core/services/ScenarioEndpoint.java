package io.nosqlbench.engine.core.services;

import io.nosqlbench.docsys.api.WebServiceObject;
import io.nosqlbench.engine.core.ScenarioController;
import io.nosqlbench.nb.api.annotations.Service;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.inject.Singleton;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Map;

@Service(WebServiceObject.class)
@Singleton
@Path("/services/nb/")
public class ScenarioEndpoint implements WebServiceObject {
    private final static Logger logger = LogManager.getLogger(ScenarioEndpoint.class);

    private ScenarioController scenarioController = new ScenarioController();

    @POST
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    @Path("run")
    public Response run(Map<String, String> activityDefMap) {
        scenarioController.run(activityDefMap);
        return Response.ok().build();
    }

}
