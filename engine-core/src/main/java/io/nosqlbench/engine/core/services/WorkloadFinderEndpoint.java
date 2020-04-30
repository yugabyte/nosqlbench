package io.nosqlbench.engine.core.services;

import io.nosqlbench.docsys.api.WebServiceObject;
import io.nosqlbench.engine.api.activityapi.core.ParameterModel;
import io.nosqlbench.engine.api.activityapi.core.ActivityType;
import io.nosqlbench.engine.api.scenarios.NBCLIScenarioParser;
import io.nosqlbench.engine.api.scenarios.WorkloadDesc;
import io.nosqlbench.engine.api.util.SimpleServiceLoader;
import io.nosqlbench.nb.api.annotations.Service;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.inject.Singleton;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Service(WebServiceObject.class)
@Singleton
@Path("/services/find/")
public class WorkloadFinderEndpoint implements WebServiceObject {
    private final static Logger logger = LogManager.getLogger(WorkloadFinderEndpoint.class);

    public static SimpleServiceLoader<ActivityType> FINDER = new SimpleServiceLoader<>(ActivityType.class);

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("workloads")
    public List<String> getWorkloadNames() {
        List<WorkloadDesc> workloads = NBCLIScenarioParser.getWorkloadsWithScenarioScripts();

        return workloads.stream().map(x -> x.getWorkloadName()).collect(Collectors.toList());
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("templates")
    public Map<String, String> getTemplatesByWorkload(@QueryParam("workloadName") String workloadName) {
        List<WorkloadDesc> workloads = NBCLIScenarioParser.getWorkloadsWithScenarioScripts();

        Map<String, String> templates = null;

        templates = workloads.stream()
            .filter(workload -> workload.getWorkloadName().equals(workloadName))
            .map(workload -> workload.getTemplates())
            .collect(Collectors.toSet()).iterator().next();

        return templates;
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("parameters")
    public List<ParameterModel> getParametersByDriver(@QueryParam("driverName") String driverName) {
        List<ActivityType> drivers = FINDER.getAll();
        ActivityType driver = drivers.stream().filter(at -> at.getName().equals(driverName)).findFirst().orElseThrow();
        List<ParameterModel> parameterModels = driver.getParameters();

        return parameterModels;
    }


}
