package pr;

/**
 * Helper class for updating program arguments between page rank jobs.  Usage: call incrementArgs
 * between jobs and use incrementArgs then getCleanupArgs for the final map-only job.
 */
public class JobArgsIterator {
  private String[] currentJobArgs;
  private String intermediateDirectory;
  private String outputDirectory;

  public JobArgsIterator(String[] programArguments) {
    currentJobArgs = new String[5];
    currentJobArgs[0] = programArguments[0]; // vertex count.
    currentJobArgs[1] = programArguments[2]; // input directory.
    currentJobArgs[2] = programArguments[3] + "/0"; // numbered output directory (for intermediate results)
    currentJobArgs[3] = "first"; // specify first iteration.
    currentJobArgs[4] = "0.0"; // dangling node mass from previous iteration.

    intermediateDirectory = programArguments[3];
    outputDirectory = programArguments[4];
  }

  public String[] getCurrentJobArgs() {
    return currentJobArgs;
  }

  public void incrementArgs(int i, double danglingMass) {
    currentJobArgs[1] = currentJobArgs[2]; // input directory is the previous iteration's output.
    currentJobArgs[3] = "not first"; // it is no longer the first iteration
    currentJobArgs[4] = "" + danglingMass;
    currentJobArgs[2] = intermediateDirectory + "/" + (i + 1); // increment the output directory.
  }

  public String[] getCleanupArgs() {
    currentJobArgs[2] = outputDirectory;
    return getCurrentJobArgs();
  }
}
