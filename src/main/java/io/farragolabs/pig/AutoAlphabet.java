package io.farragolabs.pig;

public class AutoAlphabet {

    private int characterNumber = 65;
    private int loop = 1;

    public String next()
    {
        String variableName = "";
        if(characterNumber < 91)
        {
            loop++;
        }

        return concat(String.valueOf((char)characterNumber++),loop);
    }

    private String concat(String s, int loop) {
        return s;
    }

    public static void main(String[] args) {
        AutoAlphabet autoAlphabet = new AutoAlphabet();

        for(int i=0; i < 26; i++)
        {
            System.out.println(autoAlphabet.next());
        }
    }
}
