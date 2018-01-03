package mapreduce.design.patterns.join.reduceSideJoin;

import java.io.IOException;

/**
 * Created by Sergiy Dyrda
 * <p>
 * on 18.12.2017.
 */
public class CommentReduceSideMapper extends AbstractReduceSideMapper {
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        tag = "comment";
        field = "UserId";
    }
}
