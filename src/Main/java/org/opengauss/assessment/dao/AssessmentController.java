package org.opengauss.assessment.dao;

/**
 * Assessment Module Entry
 */
public class AssessmentController implements Runnable{

    public static void startAssessment() {
        AssessmentEntry assessmentEntry = new AssessmentEntry();
        assessmentEntry.assessment();
    }

    @Override
    public void run() {
        startAssessment();
    }
}