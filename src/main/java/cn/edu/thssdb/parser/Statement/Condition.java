package cn.edu.thssdb.parser.Statement;

public class Condition {
    public String op;
    public Expression expressionLeft, expressionRight;

    public Condition(Expression expressionLeft, String op, Expression expressionRight) {
        this.expressionLeft = expressionLeft;
        this.op = op;
        this.expressionRight = expressionRight;
    }
}
