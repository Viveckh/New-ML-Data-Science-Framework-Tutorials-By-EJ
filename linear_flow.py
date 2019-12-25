from metaflow import FlowSpec, step

class LinearFlow(FlowSpec):
      
    """
    A flow to verify you can run a basic metaflow flow.
    """
    
    # Global initializations here
    
    @step
    def start(self):
        self.message = 'Thanks for reading.'
        self.next(self.process_message)

    @step
    def process_message(self):
        print('the message is: %s' % self.message)
        self.next(self.end)

    @step
    def end(self):
        print('the message is still: %s' % self.message)

if __name__ == '__main__':
    LinearFlow()