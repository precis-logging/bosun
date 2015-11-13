var Page = React.createClass({
  getInitialState(){
    return {
      url: 'about:blank'
    };
  },
  componentDidMount(){
    Loader.get('/api/v1/bosun/ui/url', function(err, url){
      this.setState({url})
    }.bind(this));
  },
  render(){
    return(
      <div className="embed-responsive embed-responsive-16by9">
        <iframe src={this.state.url} />
      </div>
    )
  }
});

Pages.register('BosunUI', Page);
